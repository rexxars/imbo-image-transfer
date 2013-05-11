(function() {

    var Queue = function(max, handler) {
        this.items    = [];
        this.handler  = handler;
        this.tasks    = 0;
        this.maxTasks = max;
    };

    Queue.prototype.add = function(item) {
        this.items.push(item);
        this.tick();
    };

    Queue.prototype.tick = function() {
        if (!this.items.length || this.tasks == this.maxTasks) {
            return;
        }

        this.process(this.items.shift());

        // Can we spawn more async tasks?
        if (this.tasks < this.maxTasks) {
            this.tick();
        }
    };

    Queue.prototype.process = function(item) {
        this.tasks++;
        this.handler(item, this.onTaskComplete.bind(this));
    };

    Queue.prototype.onTaskComplete = function() {
        this.tasks--;
        this.tick();
    };

    module.exports = Queue;

})();