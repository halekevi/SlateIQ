// *Variables*
// Declare a variable and assign it to your fav drink as a string. Make sure there is no whitespace on either side of the string, and print the value to the console

let favDrink = ' Diet vanilla Coke From Friendlys '
favDrink = favDrink.trim()
console.log(favDrink)

//Declare a variable, assign it a string of multiple words, and check to see if one of the words is "apple".
let str = ' Bod duck apple dog '
if( str.search('apple') !== -1 ){
    console.log('yes')    
}else{
    console.log('no')
}

// *Functions*
// Create a function that returns rock, paper, or scissors as randomly as possible
function rockPaperScissors (){
    let random = Math.random()
    if ( random <.33 ){
        return 'Rock'
    }else if( random <.66 ){
        return 'Paper'
    }else{
        return 'Scissors'
    }
}

// *Conditionals*
//Create a function that takes in a choice (rock, paper, or scissors) and determines if they won a game of rock paper scissors against a bot using the above function

function checkWin(playerChoice){
    let botChoice = rockPaperScissors()
    if( (playerChoice === 'Rock' && botChoice === 'Scissors') || 
    (playerChoice === 'Paper' && botChoice === 'Rock') || (playerChoice === 'Scissors' && botChoice === 'Paper') ){
        console.log( 'You Win')
    }else if (playerChoice === botChoice){
        console.log ('You Tied')
    }else{ 
        console.log('You Lose')
    }
}
checkWin('Scissors')

//*Loops*
//Create a function that takes an array of choices. Play the game x times where x is the number of choices in the array. Print the results of each game to the console.

function playGamesXTimes(arr){
    arr.forEach( choice => checkWin (choice))  
        
    }

    playGamesXTimes(['Rock', 'Paper', 'Scissors'])



    const rps = (p1, p2) => {
        if (p1 == p2)
          return 'Draw!';
          
         if (p1 == 'rock' && p2 == 'scissors') 
           return 'Player 1 won!'
         else if (p1 == 'scissors' && p2 == 'paper') 
           return 'Player 1 won!'
         else if (p1 == 'paper' && p2 == 'rock') 
           return 'Player 1 won!'
         else
           return 'Player 2 won!';
      };


        
      if ((p1 == p2)){
        return 'Draw!';   
}else if ((p1 == 'Rock' && p2 == 'Scissors') || 
     (p1 == 'Paper' && p2 == 'Rock') || 
     (p1 == 'Scissors' && p2 == 'Paper')) {
        return 'Player 1 won';
}else{    
        return 'Player 2 won!';
 }
