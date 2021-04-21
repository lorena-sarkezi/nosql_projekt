const fs = require("fs");
const { MongoClient } = require("mongodb");
const { Helpers } = require("./Helpers");

const databaseUri = "mongodb://localhost:27017";
const client = new MongoClient(databaseUri, {
    useNewUrlParser: true,
    useUnifiedTopology: true
});

const main = async () => {

    try {
        await client.connect();
        const database = client.db("nosql");
        const csvDbCollection = database.collection("csvData");
        try{
            await csvDbCollection.drop();
        }
        catch(err){
        }
        

        const csvString = fs.readFileSync("./HTRU_2.csv", "utf-8");

        let parsedCsvObject = Helpers.csvToJSON(csvString);

        // 1. Zadatak - obrada nedostajućih kategoričkih i kontinuiranih vrijednosti
        // 2. zadatak, treći dio - broj non-Null vrijednosti za svaku kontinuiranu varijablu

        let nullValsCount = {
            profileMean: 0,
            profileStdDeviation: 0,
            profileExcessKurtosis: 0,
            profileSkewness: 0,
            dmSnrCurveMean: 0,
            dmSnrCurveStdDeviation: 0,
            dmSnrCurveExcessKurtosis: 0,
            dmSnrCurveSkewness: 0
        }

        parsedCsvObject.forEach((element, index) => {
            if (element.profileMean === null || element.profileMean === undefined || element.profileMean === "") {
                nullValsCount.profileMean++;
                element.profileMean = -1;
            }
            if (element.profileStdDeviation === null || element.profileStdDeviation === undefined || element.profileStdDeviation === "") {
                nullValsCount.profileStdDeviation++;
                element.profileStdDeviation = -1;
            }
            if (element.profileExcessKurtosis === null || element.profileExcessKurtosis === undefined || element.profileExcessKurtosis === "") {
                nullValsCount.profileExcessKurtosis++;
                element.profileExcessKurtosis = -1;
            }
            if (element.profileSkewness === null || element.profileSkewness === undefined || element.profileSkewness === "") {
                nullValsCount.profileSkewness++;
                element.profileSkewness = -1;
            }
            if (element.dmSnrCurveMean === null || element.dmSnrCurveMean === undefined || element.dmSnrCurveMean === "") {
                nullValsCount.dmSnrCurveMean++;
                element.dmSnrCurveMean = -1;
            }
            if (element.dmSnrCurveStdDeviation === null || element.dmSnrCurveStdDeviation === undefined || element.dmSnrCurveStdDeviation === "") {
                nullValsCount.dmSnrCurveStdDeviation++;
                element.dmSnrCurveStdDeviation = -1;
            }
            if (element.dmSnrCurveExcessKurtosis === null || element.dmSnrCurveExcessKurtosis === undefined || element.dmSnrCurveExcessKurtosis === "") {
                nullValsCount.profileExcessKurtosis++;
                element.dmSnrCurveExcessKurtosis = -1;
            }
            if (element.dmSnrCurveSkewness === null || element.dmSnrCurveSkewness === undefined || element.dmSnrCurveSkewness === "") {
                nullValsCount.dmSnrCurveSkewness++;
                element.dmSnrCurveSkewness = -1;
            }

            if (element.class === null || element.class === undefined || element.class === "") {
                element.class = "empty";
            }
        });

        await csvDbCollection.insertMany(parsedCsvObject);

        // 2. zadatak, prvi dio - srednje vrijednosti za sve kontinuirane varijable
        const avgResult = await csvDbCollection.aggregate(
            [
                {
                    $group: {
                        _id: null,
                        Average_ProfileMean: {"$avg": '$profileMean'},
                        Average_ProfileStdDeviation: {"$avg":"$profileStdDeviation"},
                        Average_profileExcessKurtosis: {"$avg":"$profileExcessKurtosis"},
                        Average_profileSkewness: {"$avg":"$profileSkewness"},
                        Average_dmSnrCurveMean: {"$avg":"$dmSnrCurveMean"},
                        Average_dmSnrCurveStdDeviation: {"$avg":"$dmSnrCurveStdDeviation"},
                        Average_dmSnrCurveExcessKurtosis: {"$avg":"$dmSnrCurveExcessKurtosis"},
                        Average_dmSnrCurveSkewness: {"$avg":"$dmSnrCurveSkewness"}
                    }
                }
            ]
        );

        // 2. zadatak, drugi dio - standardna devijacija za sve kontinuirane varijable
        const stdDevResult = await csvDbCollection.aggregate(
            [
                {
                    $group: {
                        _id: null,
                        StdDevSamp_ProfileMean: {"$stdDevSamp": '$profileMean'},
                        StdDevSamp_ProfileStdDeviation: {"$stdDevSamp":"$profileStdDeviation"},
                        StdDevSamp_profileExcessKurtosis: {"$stdDevSamp":"$profileExcessKurtosis"},
                        StdDevSamp_profileSkewness: {"$stdDevSamp":"$profileSkewness"},
                        StdDevSamp_dmSnrCurveMean: {"$stdDevSamp":"$dmSnrCurveMean"},
                        StdDevSamp_dmSnrCurveStdDeviation: {"$stdDevSamp":"$dmSnrCurveStdDeviation"},
                        StdDevSamp_dmSnrCurveExcessKurtosis: {"$stdDevSamp":"$dmSnrCurveExcessKurtosis"},
                        StdDevSamp_dmSnrCurveSkewness: {"$stdDevSamp":"$dmSnrCurveSkewness"}
                    }
                }
            ]
        );

        const statistika_HTRU_2 = {
            statAverage: (await avgResult.toArray())[0],
            statStdDeviation: (await stdDevResult.toArray())[0]
        };

        console.log("Statistika:")
        console.log(statistika_HTRU_2);
        
        const statistika_HTRU_2_collection = database.collection("statistika_HTRU_2");
        await statistika_HTRU_2_collection.insertOne(statistika_HTRU_2);

        //2. zadatak, treći dio - broj non-Null vrijednosti za svaku kontinuiranu varijablu
        const nonNullValsCount = {
            profileMean: parsedCsvObject.length - nullValsCount.profileMean,
            profileStdDeviation: parsedCsvObject.length - nullValsCount.profileStdDeviation,
            profileExcessKurtosis: parsedCsvObject.length - nullValsCount.profileExcessKurtosis,
            profileSkewness: parsedCsvObject.length - nullValsCount.profileSkewness,
            dmSnrCurveMean: parsedCsvObject.length - nullValsCount.dmSnrCurveMean,
            dmSnrCurveStdDeviation: parsedCsvObject.length - nullValsCount.dmSnrCurveStdDeviation,
            dmSnrCurveExcessKurtosis: parsedCsvObject.length - nullValsCount.dmSnrCurveExcessKurtosis,
            dmSnrCurveSkewness: parsedCsvObject.length - nullValsCount.dmSnrCurveSkewness
        };

        console.log("Non-null vals count:");
        console.log(nonNullValsCount);



        // 3. Zadatak - frekvencija kategoričkih varijabli
        const frequency_0 = await csvDbCollection.aggregate(
            [
                {
                    $match:{
                        class: 0
                    }
                },
                {
                    $count: "0"
                }
            ]
        );

        const frequency_1 = await csvDbCollection.aggregate(
            [
                {
                    $match:{
                        class: 1
                    }
                },
                {
                    $count: "1"
                }
            ]
        );

        
        
        let frekvencija_HTRU_2 = [];
        frekvencija_HTRU_2.push((await frequency_0.toArray())[0]);
        frekvencija_HTRU_2.push((await frequency_1.toArray())[0]);
        
        console.log("\n\n----------------------------------------------------------------\n\n");
        console.log("Frekvencija:")
        console.log(frekvencija_HTRU_2);
    }
    finally {
        client.close();
    }
}


main().catch(console.dir);