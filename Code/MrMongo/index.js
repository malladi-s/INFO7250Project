var map = function() {
    emit(this.ZIP, { count: 1 });
};

var reduce = function(key, values) {
    var result = { count: 0 };
    values.forEach(function(value) {
        result.count += value.count;
    });
    return result; 
};

db.MR0.find().sort({"value.count": -1}).pretty();