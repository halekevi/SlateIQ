//Write your pseduo code first! 
// 0 -> 32
document.querySelector('#yell').addEventListener('click', convert)

function convert() {
//nedd the value that's in celsius

let temp = document.querySelector('#cel').value

//convert from celsius 2 fahrenheit

temp = temp * 9/5 + 32

// and then show it

document.querySelector('#placeToYell').innerText = temp