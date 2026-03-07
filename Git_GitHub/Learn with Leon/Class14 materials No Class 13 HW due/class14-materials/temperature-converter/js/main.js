//Write your pseduo code first!

//only click
document.querySelector('#temp').addEventListener('click', run)

function run() {
    console.log('Hello Twitch');
    //get value out of input
    let temp = document.querySelector('#temp').value
    //convert the value
    temp = temp * 9/5 + 32
    //show value to the user
    document.querySelector('#temp').innerText = temp
}
