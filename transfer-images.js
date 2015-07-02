var cluster = require('cluster')
  , Imbo    = require('imboclient')
  , Queue   = require('./queue')
  , numCpus = require('os').cpus().length
  , verbose = process.argv.indexOf('--verbose') > -1
  , config;

try {
    config = require('./config.json');
} catch (e) {
    if (e.code == 'MODULE_NOT_FOUND') {
        console.log('[ERROR] Please copy config.json.dist to config.json and adjust values');
        return;
    } else {
        throw e;
    }
}

var sinceAt   = process.argv.indexOf('--since')
  , since     = null
  , batchSize = 100;

if (sinceAt > -1 && process.argv.length > sinceAt) {
    since = parseInt(process.argv[sinceAt + 1], 10);
    since = isNaN(since) ? null : since;
}

(function() {
    if (cluster.isMaster) {
        var workers          = []
          , totalImages      = 0
          , completeHandlers = {}
          , imagesAdded      = 0
          , imagesErrored    = 0
          , imagesSkipped    = 0
          , imageQueryDone   = false;

        // Checks if the queue is completed
        var checkComplete = function() {
            if (!imageQueryDone) {
                return false;
            }

            var working = false;
            for (var key in completeHandlers) {
                working = working || completeHandlers[key];
            }

            if (!working) {
                // Kill workers
                for (var i = 0; i < workers.length; i++) {
                    workers[i].destroy();
                }

                console.log('All done!');
                console.log('Added  : ' + imagesAdded);
                console.log('Skipped: ' + imagesSkipped);
                console.log('Errors : ' + imagesErrored);
            }
        };

        // Completeness-calculator
        var getProgress = function() {
            return '[' + (imagesAdded + imagesSkipped + imagesErrored) + ' / ' + totalImages + ']';
        };

        // Callback from workers
        var onWorkerMessage = function(msg) {
            if (msg.type == 'complete') {
                imagesAdded++;
                if (verbose) {
                    console.log(getProgress() + ' ' + msg.id + ' completed');
                }
            } else if (msg.type == 'already-exists') {
                imagesSkipped++;
                if (verbose) {
                    console.log(getProgress() + ' ' + msg.id + ' already exists on target');
                }
            } else if (msg.type == 'error') {
                imagesErrored++;
                console.log(getProgress() + ' ' + msg.id + ' failed - ' + msg.error);
            }

            if (completeHandlers[this.id]) {
                completeHandlers[this.id]();
                completeHandlers[this.id] = null;
            }

            checkComplete();
        };

        // Queue management
        for (var i = 0; i < numCpus; i++) {
            workers.push(cluster.fork());
            completeHandlers[workers[i].id] = null;

            workers[i].on('message', onWorkerMessage);
        }

        // Prepare a queue
        var currentWorker = 0;
        var queue = new Queue(
            Math.max(numCpus - 1, 1),
            function(item, onComplete) {
                workers[currentWorker].send(item);
                completeHandlers[workers[currentWorker].id] = onComplete;

                // Reset current worker to zero if we reached last worker
                if (++currentWorker == workers.length - 1) {
                    currentWorker = 0;
                }
            }
        );

        // Instantiate source client and query
        var from  = new Imbo.Client(config.from.host, config.from.publicKey, config.from.privateKey)
          , query = new Imbo.Query().limit(batchSize)
          , page  = 0;

        // Since a specific date?
        if (since) {
            query.from(new Date(since));
        }

        // Fetch images from source
        var onImageBatchFetched = function(err, images) {
            if (err) {
                return failed++;
            }

            totalImages += images.length;

            for (var i = 0; i < images.length; i++) {
                queue.add([
                    images[i].imageIdentifier,
                    images[i].extension
                ]);
            }

            console.log(images.length, batchSize);
            if (images.length == batchSize) {
                // Not done yet, fetch next batch
                page++;
                getNextImageBatch();
            } else {
                imageQueryDone = true;
                console.log('All image IDs fetched, waiting for queue to finish');
            }
        };
        var getNextImageBatch = function() {
            query.page(page);

            if (verbose) {
                console.log('Fetching image batch');
            }

            from.getImages(query, onImageBatchFetched);
        };

        // Start fetching
        getNextImageBatch();
    } else {

        // Instantiate clients
        var source      = new Imbo.Client(config.from.host, config.from.publicKey, config.from.privateKey)
          , destination = new Imbo.Client(config.to.host,   config.to.publicKey,   config.to.privateKey)
          , url;

        // Handler for the image-transfer flow
        var imboHandler = {
            setImageExtension: function(ext) {
                this.extension = ext;
            },

            setImageIdentifier: function(id) {
                this.id = id;
            },

            run: function() {
                destination.imageIdentifierExists(
                    this.id,
                    this.onImageExists.bind(this)
                );
            },

            onImageExists: function(err, exists) {
                if (exists) {
                    return this.complete('already-exists');
                }

                url = source.getImageUrl(this.id).convert(this.extension);
                destination.addImageFromUrl(
                    url,
                    this.onAddImageResponse.bind(this)
                );
            },

            onAddImageResponse: function(err, res) {
                if (err) {
                    return this.complete('error', err);
                }

                destination.getMetadata(
                    this.id,
                    this.onGetMetadata.bind(this)
                );
            },

            onGetMetadata: function(err, data) {
                if (err) {
                    return this.complete('error', err);
                }

                destination.replaceMetadata(
                    this.id,
                    data,
                    this.onReplaceMetadata.bind(this)
                );
            },

            onReplaceMetadata: function(err) {
                this.complete('complete', err);
            },

            complete: function(type, err) {
                process.send({
                    type : type,
                    id   : this.id,
                    error: err
                });
            }
        };

        process.on('message', function(item) {
            imboHandler.setImageExtension(item[1]);
            imboHandler.setImageIdentifier(item[0]);

            imboHandler.run();
        });

    }

})();
