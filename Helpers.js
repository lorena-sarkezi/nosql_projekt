function csvToJSON(csvString) {

    var lines = csvString.split("\r");

    var result = [];

    var headers = lines[0].split(",");

    for (var i = 1; i < lines.length; i++) {

        var obj = {};
        var currentline = lines[i].split(",");

        for (var j = 0; j < headers.length; j++) {
            obj[headers[j]] = currentline[j];
        }

        result.push(obj);

    }

    result.forEach((element, index) => {
        element.profileMean = parseFloat(element.profileMean);
        element.profileStdDeviation = parseFloat(element.profileStdDeviation);
        element.profileExcessKurtosis = parseFloat(element.profileExcessKurtosis);
        element.profileSkewness = parseFloat(element.profileSkewness);
        element.dmSnrCurveMean = parseFloat(element.dmSnrCurveMean);
        element.dmSnrCurveStdDeviation = parseFloat(element.dmSnrCurveStdDeviation);
        element.dmSnrCurveExcessKurtosis = parseFloat(element.dmSnrCurveExcessKurtosis);
        element.dmSnrCurveSkewness = parseFloat(element.dmSnrCurveSkewness);
        element.class = parseInt(element.class);
    });

    return result; 
}

const Helpers = {
    csvToJSON: csvToJSON
}
module.exports = {
    Helpers: Helpers
}