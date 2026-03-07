function repeat(n,action){
    for (let i = 0; i<n;i++){
        action(i);
    }
}
repeat(3,console.log);

//
let total = 0, count = 1;
while (count<=10){
    total += count;
    count+=1;
}
console.log(total);

let labels = [];
repeat(5,i=>{
    labels.push(`Kev${i+1}`);
});
console.log(labels);

function greaterThan(n){
    return m =>n;
}
let greaterThan10= greaterThan(10);
console.log(greaterThan10(11));