//FUNCTIONS

const square = function(x){
    return x*x;
};
console.log(square(12));
 /* **********************************************/

const makeNoise = function(){
    console.log("Pling!");
};

makeNoise();
 /* **********************************************/
  const power = function(base, exponent){
    let result = 1;
    for (let count = 0; count <= exponent; count++){
        result *= base;
    }
    return result;
  };
  console.log(power(2,9));
   /* *************LEXICAL SCOPING*********************************/

const hummus = function(factor) {
    const ingredient = function(amount, unit,name){
        let ingredientAmount = amount*factor;
        if(ingredientAmount > 1) {
            unit +="s"
        }
        console.log('${ingredientAmount} ${unit} ${name');
    };
    ingredient(1,"can", "chickpeas");
};
   /* *************LEXICAL SCOPING*********************************/
   /*let launchMissles = function(){
    missileSystem.launch("now");
   };
   if(safeMode){
    //launchMissiles = function(){Do Nothing}; 
//}
/* *************ARROW FUNCTION*********************************/
  
   const sq1 = (x) => {return X*X};
   const sq2 = (x) =>  X*X;
   /* ********************************************************/
   function minus(a,b){
    if(b===undefined) return -a;
        else return a-b;
   }
   console.log(minus(10,5));

   /* *******************WRAP VALUES*************************************/
   function wrapValue(n){
    let local = n;
    return() =>local;
   }

   let wrap1 = wrapValue(1);
   let wrap2 = wrapValue(2);
   console.log(wrap1());

   console.log(wrap2());
/* ********************************************************/
function multiplier(factor){
    return number => number*factor;0
}

let twice = multiplier(2);
console.log(twice(5));
/* *****RECURSION*****************************************/
/* *****recursive version of above INEFFICIENT!!!! (functions that call on itself)*****************************************/
function power2(base,exponent) {
    if (exponent == 0){
        return 1;
    } else{
        return base*power(base,exponent-1);
    }
}
console.log(power2(2,3));

function findSolution(target){
    function find(current,history){
        if(current==target){
            return history;
        }else if (current > target){
            return null;
        }else{
            return find(current + 5, `(${history} +5)`) || 
                find(current*3, `(${history} *3)`);
        }
    }
    return find(1,"1");
}
console.log(findSolution(13));
/* *****Growing Functions*****************************************/
function printFarmInventory(cows,chickens){
    let cowString = String(cows);
    while (cowString.length < 3) {
        cowString = "0" +cowString;
    }
    console.log(`${cowString}Cows`);
    let chickenString = String(chickens);
    while (chickenString.length < 3){
        chickenString = "0"+chickenString;
    }
    console.log(`${chickenString} Chickens`);
    }
    printFarmInventory(7,11)
;

//UPDATED VERSION
function printZeroPaddedWithLabel(number,label){
    let numberString = String(number);
    while (numberString.length < 3){
     numberString = "0"+ numberString;
    }
    console.log(`${numberString} ${label}`);
    }
    function printFarmInventory(cows,chickens,pigs){
        printZeroPaddedWithLabel(cows,"Cows");
        printZeroPaddedWithLabel(chickens,"Chickens");
        printZeroPaddedWithLabel(pigs,"Pigs");
    }
    printFarmInventory(7,11,3);

function zeroPad(number,width){
    let string = String(number);
    while(string.length < width){
        string="0"+string;
    }
    return string;
}
function printFarmInventory(cows,chickens,pigs){
    console.log(`${zeroPad(cows,3)}Cows`);
    console.log(`${zeroPad(chickens,3)}Chickens`);
    console.log(`${zeroPad(pigs,3)}Pigs`);
}
printFarmInventory(7,16,3);

//Define f to hold a function vlaue
const f = function(a){
    console.log(a+2);
}
//Decalre g to be a function
function g(a,b){
    return a*b*3.5;
}

//A less verbose function value
let h = a => a%3;

//EXERCISES

function min(a,b){
    return Math.min(a,b);
};
console.log(min(100,50));

function isEven(x) {
    if (x%2 === 0){
        return "Even";
    }else if (x%2 === 1){
        return "Odd";
    }else if ((-1*x)%2===0){
        return "Even";
    }else if ((-1*x)%2===1){
        return "Odd";
    }else{
        return (x === x-2);
    };
}
console.log(isEven(75));

function countBs(string){
        let count = 0;
        for (let char of string) {
            // If the character is a capital 'B', increment the counter
            if (char === 'B') {
                count++;
            }
        }
        // Return the count of capital 'B's
        return count;
    }
    // Example usage
    console.log(countBs("Beautiful Butterflies Brought Blue Balloons")); // Output: 4
    console.log(countBs("No capital B here")); 