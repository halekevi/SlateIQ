let day1 = {
    squirrel: false,
    events:["work", "touched tree","pizza", "running"]
};
console.log(day1.squirrel);
console.log(day1.wolf);

day1.wolf = false;
console.log(day1.wolf);

let descriptions = {
    work: "Went to work",
    "touched tree": "Touched a tree"
};
//
let anObject = {left: 1, right: 2};
console.log(anObject.left);
delete anObject.left // deleed left
console.log("left"in anObject);
console.log('right'in anObject);

//object.keys returns object porperties as an array of strings
console.log(Object.keys({x:0,y:0,z:2}));

//object.assign
let objectA = {a:1,b:2};
Object.assign(objectA, {b:3,c:4});
console.log(objectA);

let journal = [
    {events:["work", "touched tree","pizza", "running", "television"],
    squirrel: false},
    {events:["work", "ice cream", "cauliflower", "lasagna", "touched tree", "brushed teeth"],   
    squirrel:false},
    {events:["weekend", "cycling","break","peanuts","beer"],
    squirrel:true},
];

let object1 = {value:10};
let object2 = object1;
let object3 = {value:10};

console.log(object1==object2);
console.log(object1==object3);

object1.value=15;
console.log(object2.value);
console.log(object3.value);