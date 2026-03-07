/*
const square = function(x) {
    return x*x
};

console.log(square(11));
*/

const power = function(base,exponent) {
    let result = 1;
    for (let count = 0; count <exponent; count++){
        result *= base;
    }
    return result;
};

console.log(power(2,5));

let x = 10;
if(true){
    let y = 20;
    var z  = 30;
    console.log (x + y + z);
    console.log (x + z);

    
    console.log("The future says:", future());

    function future(){
        return "you'll never have flying cars";
    }
}

function square(x) {
    return x*x;
}

//ARROW FUNCS//
//           //
const horn = () => {
        console.log("Toot");
    };
//

function wrapValue(n) {
        let local = n;
        return() => local;
}

let wrap1= wrapValue(1);
console.log(wrap1());

function greet(name, lastName) {
    console.log('Hello ' + name + '' + lastName);
}

greet ('Kevin ', "Hale");

function chicken() {
    return egg();
}

function egg() {
    return chicken();
}

console.log(chicken() + "came first.");