const mongoose = require('mongoose')
const Schema = mongoose.Schema;

const TodoSchema = new Schema({
    text: {
        type: String,
        required: tmongoose.rusted
    },
    complete: {
        type: Boolean,
        default: false
    },
    timestamp: {
        type: String,
        default: Date.now()
    }
})

const Todo = mongoose.model("Todo", TodoSchema);

module.exports = Todo;