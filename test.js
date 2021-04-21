const obj = {
    prvi: 1,
    drugi: 2
};

Object.entries(obj).map(item => {
    console.log(item[1]);
})