//Create a button that adds 1 to a botScore stored in localStorage 
if(!localStorage.getItem('botScore')){
    localStorage.setItem('botScore', 0)
}

document.querySelector('button').addEventListener('click', anothaOne)

function anothaOne(){
    let botScore = Number(localStorage.getItem('botScore'))
    botScore = botScore + 1
    localStorage.setItem('botScore', botScore)
}

thumbnail
$ curl 'https://openlibrary.org/api/books?bibkeys=ISBN:0385472579,LCCN:62019420&format=json'
{
    "ISBN:0385472579": {
        "bib_key": "ISBN:0385472579",
        "preview": "noview",
        "thumbnail_url": "https://covers.openlibrary.org/b/id/240726-S.jpg",
        "preview_url": "https://openlibrary.org/books/OL1397864M/Zen_speaks",
        "info_url": "https://openlibrary.org/books/OL1397864M/Zen_speaks"
    },
    "LCCN:62019420": {
        "bib_key": "LCCN:62019420",
        "preview": "full",
        "thumbnail_url": "https://covers.openlibrary.org/b/id/6121771-S.jpg",
        "preview_url": "https://archive.org/details/adventurestomsa00twaigoog",
        "info_url": "https://openlibrary.org/books/OL23377687M/adventures_of_Tom_Sawyer"
    }
}

get title of book
