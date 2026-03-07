let listOfNumbers = [2, 3, 5, 7, 11];
console.log(listOfNumbers[2]);
console.log(listOfNumbers[0]);
console.log(listOfNumbers[2-1]);

let doh = "Doh";
console.log(typeof doh.toUpperCase);
console.log(doh.toUpperCase());

let sequence = [1, 2, 3];
sequence.push(4); // gives new array w added value
sequence.push(5);
console.log(sequence);
console.log(sequence.pop()); // prints added/removed value
console.log(sequence);


/*function tableFor(event, journal) {
    let table = [0, 0, 0, 0];
    for (let i = 0; i < journal.length; i++) {
      let entry = journal[i], index = 0;
      if (entry.events.includes(event)) index += 1;
      if (entry.squirrel) index += 2;
      table[index] += 1;
    }
    return table;
  }*/

  // Simpler Version of above
//for (let entry of JOURNAL){ //looping over element after of aka JOURNAL
  //  console.log(`${entry.events.length}events.`);
//}

//.trim removes whitspaces
//.padStart(3,"0"); pads variables to string/value x = length of padding & y = value to pad with
//.split(""); splits values into arrays
//.join("."); joins arrays with value (.)
///repeat("x"); repeats inserted value x number of times
//Math.abs(); takes absolute value of a number

// INSERT ARRAY in an ARRAY
let word = ["OH", "HELL", "NAWL"];
console.log([...word, "You", "DID", "What!!!"]);

function randomPointoNCircle(radius){
    let angle = Math.random()*2*Math.PI;
    return {x:radius*Math.cos(angle),
        y:radius*Math.sin(angle)};
    }
console.log(randomPointoNCircle(2));
console.log(Math.floor(Math.random()*10));
