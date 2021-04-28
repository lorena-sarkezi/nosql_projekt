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
        try{
            await client.db("nosql").dropDatabase();
        }
        catch(err){
        }

        const database = client.db("nosql");
        const csvDbCollection = database.collection("csvData");
        
        

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

        // 2. zadatak, prvi dio - srednje/prosječne vrijednosti za sve kontinuirane varijable
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

        const frekvencijaCollection = database.collection("frekvencija_HTRU_2");
        await frekvencijaCollection.insertMany(frekvencija_HTRU_2);

        
        
        console.log("\n\n----------------------------------------------------------------\n\n");
        console.log("Frekvencija:")
        console.log(frekvencija_HTRU_2);


        // 4. zadatak
        // Svaki objekt statistike ima N property-ja, gdje je N broj kontinuiranih vrijednosti. Svaki property je array brojeva, odnosno vrijednosti iz originalnog seta podataka
        // Property-ji objekata statistike sadržavaju odgovarajuće vrijednosti odgovarajućeg elementa (npr. profileMean) koji odgovara danom uvjetu

        let statistika1_HTRU_2 = {
            profileMean: [],
            profileStdDeviation: [],
            profileExcessKurtosis: [],
            profileSkewness: [],
            dmSnrCurveMean: [],
            dmSnrCurveStdDeviation: [],
            dmSnrCurveExcessKurtosis: [],
            dmSnrCurveSkewness: []
        };
        let statistika2_HTRU_2 = {
            profileMean: [],
            profileStdDeviation: [],
            profileExcessKurtosis: [],
            profileSkewness: [],
            dmSnrCurveMean: [],
            dmSnrCurveStdDeviation: [],
            dmSnrCurveExcessKurtosis: [],
            dmSnrCurveSkewness: []
        };

        parsedCsvObject.forEach((element, index) => {
            // profileMean
            if(element.profileMean <= statistika_HTRU_2.Average_ProfileMean){
                statistika1_HTRU_2.profileMean.push(element.profileMean);
            }
            else{
                statistika2_HTRU_2.profileMean.push(element.profileMean);
            }

            // profileStdDeviation
            if(element.profileStdDeviation <= statistika_HTRU_2.Average_ProfileStdDeviation){
                statistika1_HTRU_2.profileStdDeviation.push(element.profileStdDeviation);
            }
            else{
                statistika2_HTRU_2.profileStdDeviation.push(element.profileStdDeviation);
            }

            // profileExcessKurtosis
            if(element.profileExcessKurtosis <= statistika_HTRU_2.Average_profileExcessKurtosis){
                statistika1_HTRU_2.profileExcessKurtosis.push(element.profileExcessKurtosis);
            }
            else{
                statistika2_HTRU_2.profileExcessKurtosis.push(element.profileExcessKurtosis);
            }

            // profileSkewness
            if(element.profileSkewness <= statistika_HTRU_2.Average_profileSkewness){
                statistika1_HTRU_2.profileSkewness.push(element.profileSkewness);
            }
            else{
                statistika2_HTRU_2.profileSkewness.push(element.profileSkewness);
            }

            // dmSnrCurveMean
            if(element.dmSnrCurveMean <= statistika_HTRU_2.Average_dmSnrCurveMean){
                statistika1_HTRU_2.dmSnrCurveMean.push(element.dmSnrCurveMean);
            }
            else{
                statistika2_HTRU_2.dmSnrCurveMean.push(element.dmSnrCurveMean);
            }

            // dmSnrCurveStdDeviation
            if(element.dmSnrCurveStdDeviation <= statistika_HTRU_2.Average_dmSnrCurveStdDeviation){
                statistika1_HTRU_2.dmSnrCurveStdDeviation.push(element.dmSnrCurveStdDeviation);
            }
            else{
                statistika2_HTRU_2.dmSnrCurveStdDeviation.push(element.dmSnrCurveStdDeviation);
            }

            // dmSnrCurveExcessKurtosis
            if(element.dmSnrCurveExcessKurtosis <= statistika_HTRU_2.Average_dmSnrCurveExcessKurtosis){
                statistika1_HTRU_2.dmSnrCurveExcessKurtosis.push(element.dmSnrCurveExcessKurtosis);
            }
            else{
                statistika2_HTRU_2.dmSnrCurveExcessKurtosis.push(element.dmSnrCurveExcessKurtosis);
            }

            // dmSnrCurveSkewness
            if(element.dmSnrCurveSkewness <= statistika_HTRU_2.Average_dmSnrCurveSkewness){
                statistika1_HTRU_2.dmSnrCurveSkewness.push(element.dmSnrCurveSkewness);
            }
            else{
                statistika2_HTRU_2.dmSnrCurveSkewness.push(element.dmSnrCurveSkewness);
            }
        });

        const statistika1Collection = database.collection("statistika1_HTRU_2");
        await statistika1Collection.insertOne(statistika1_HTRU_2);

        const statistika2Collection = database.collection("statistika2_HTRU_2");
        await statistika2Collection.insertOne(statistika2_HTRU_2);


        // 5. zadatak 
        let emb_HTRU_2 = await csvDbCollection.find().toArray();
    
        emb_HTRU_2.forEach((element, index) => {
            let tempclassVal = element.class;

            //vrijednost moze biti samo 0 ili 1 za property "class"
            if(element.class == 0){
                element.class = {
                    value: tempclassVal,
                    frequency_emb: frekvencija_HTRU_2[0]._id
                }
            }
            else {
                element.class = {
                    value: tempclassVal,
                    frequency: frekvencija_HTRU_2[1]._id
                }
            }
        });

        const embCollection = database.collection("emb_HTRU_2");
        await embCollection.insertMany(emb_HTRU_2);


        // 6. zadatak
        let emb2_HTRU_2 = await csvDbCollection.find().toArray();    

        emb2_HTRU_2.forEach((element, index) => {
            element.profileMean = {
                value: element.profileMean,
                statistics: statistika_HTRU_2._id
            };

            element.profileStdDeviation = {
                value: element.profileStdDeviation,
                statistics: statistika_HTRU_2._id
            }

            element.profileExcessKurtosis = {
                value: element.profileExcessKurtosis,
                statistics: statistika_HTRU_2._id
            }

            element.profileSkewness = {
                value: element.profileSkewness,
                statistics: statistika_HTRU_2._id
            }

            element.dmSnrCurveMean = {
                value: element.dmSnrCurveMean,
                statistics: statistika_HTRU_2._id
            }

            element.dmSnrCurveStdDeviation = {
                value: element.dmSnrCurveStdDeviation,
                statistics: statistika_HTRU_2._id
            }

            element.dmSnrCurveExcessKurtosis = {
                value: element.dmSnrCurveExcessKurtosis,
                statistics: statistika_HTRU_2._id
            }

            element.dmSnrCurveSkewness = {
                value: element.dmSnrCurveSkewness,
                statistics: statistika_HTRU_2._id
            }
        });
        
        const emb2Collection = database.collection("emb2_HTRU_2");
        await emb2Collection.insertMany(emb2_HTRU_2);


        // 8. zadatak
        csvDbCollection.createIndex({"class":1,"profileMean":-1});
        const indexQuery = [
            {
                $match: {
                    $and: [
                            { class: { $eq: 0 } }, 
                            { profileMean: { $gte: statistika_HTRU_2.statStdDeviation.StdDevSamp_ProfileMean } }
                        ]
                }
            }
        ];
        const indexedCollectionResult = await csvDbCollection.aggregate(indexQuery).toArray();
        const indexedCollection = database.collection("indexed_filtered");
        await indexedCollection.insertMany(indexedCollectionResult);

    }
    finally {
        client.close();
    }
}


main().catch(console.log);