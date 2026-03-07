//Example fetch using pokemonapi.co
document.querySelector('button').addEventListener('click', getFetch)

function getFetch(){
  const choice = document.querySelector('input').value
  console.log(choice)
  const url = 'https://api.nasa.gov/planetary/apod?api_key=FD63OAHnX9r7ycMxWjpeUAwB7dl3sHYC8LpisILr'=$choice

  fetch(url)
      .then(res => res.json()) // parse response as JSON
      .then(data => {
        console.log(data)
        if( data.media_type === 'image'){
          document.querySelector('img').src = data.hdurl
        }else if (data.media_type ===video)
        document.querySelector('img').src = data.hurl
        document.querySelector('h3').innertext = data.explanation
      })
      .catch(err => {
          console.log(`error ${err}`)
      });
}

