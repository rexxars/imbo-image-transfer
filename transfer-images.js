var cluster = require('cluster')
  , Imbo    = require('imbo-client')
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

        // Callback from workers
        var onWorkerMessage = function(msg) {
            if (msg.type == 'complete' && verbose) {
                console.log(msg.id + ' completed');
                imagesAdded++;
            } else if (msg.type == 'already-exists' && verbose) {
                console.log(msg.id + ' already exists on target');
                imagesSkipped++;
            } else if (msg.type == 'error') {
                console.log(msg.id + ' failed - ' + msg.error);
                imagesErrored++;
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

            for (var i = 0; i < images.length; i++) {
                queue.add([
                    images[i].imageIdentifier,
                    images[i].extension
                ]);
            }

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
            from.getImages(onImageBatchFetched, query);
        };

        // Start fetching
        getNextImageBatch();
    } else {

        // Instantiate clients
        var source      = new Imbo.Client(config.from.host, config.from.publicKey, config.from.privateKey)
          , destination = new Imbo.Client(config.to.host,   config.to.publicKey,   config.to.privateKey)
          , url;

        process.on('message', function(item) {
            destination.imageIdentifierExists(item[0], function(err, exists) {
                if (exists) {
                    return process.send({ type: 'already-exists', id: item[0] });
                }

                // Image does not exist, add it
                url = source.getImageUrl(item[0]).convert(item[1]);
                destination.addImageFromUrl(url, function(err, res) {
                    process.send({ type: (err ? 'error' : 'complete'), id: item[0], error: err });
                });
            });
        });

    }

})();