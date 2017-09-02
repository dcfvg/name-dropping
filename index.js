const fs = require('fs');
const http = require('http');

const transform = require('stream-transform');
const parse = require('csv-parse');
const parser = parse();

const wikipedia = require("node-wikipedia");

var inputFile = fs.createReadStream(__dirname+'/entities_list-test.csv');
var outputFile = fs.createWriteStream(__dirname+'/output.csv');

var transformer = transform(function(data, callback){

  console.log('\n'+data[0]);

  new Promise(function(fulfill, reject){

    // search for a wiki-fr page
    wikipedia.page.data(encodeURIComponent(data[0]), { content: false, lang:'fr' }, function(response) {
      data[5] = typeof response !== "undefined";
      if(typeof response !== "undefined") console.log('wiki en:',response.title);
      fulfill(data);
    });

  }).then(function(result){
    return new Promise(function(fulfill, reject){

      // search for a wiki-en page
      wikipedia.page.data(encodeURIComponent(data[0]), { content: false, lang:'en' }, function(response) {
        data[7] = typeof response !== "undefined";
        if(typeof response !== "undefined") console.log('wiki en:',response.title);
        fulfill(data);
      });

    });
  }).then(function(result){
    return new Promise(function(fulfill, reject){

      // count avaible books in Paris
      var url = 'http://opendata.paris.fr/api/records/1.0/search/?dataset=tous-les-documents-des-bibliotheques-de-pret&rows=-1&q='+encodeURIComponent(data[0]);
      http.get(url, function(res){
        var body = '';
        res.on('data', function(chunk){ body += chunk });
        res.on('end', function(){
          var response = JSON.parse(body);
          if(typeof response.records !== "undefined"){
            console.log(response.records.length, "books");
            data[8] = response.records.length;
          }else{data[8] = -1}
          fulfill(data);
        });
      }).on('error', function(e){console.log("Got an error: ", e) });
    });
  }).then(function(result){
      return new Promise(function(fulfill, reject){

        // count for past events in Paris
        var url = 'http://opendata.paris.fr/api/records/1.0/search/?dataset=evenements-a-paris&rows=-1&q='+encodeURIComponent(data[0]);
        http.get(url, function(res){
          var body = '';
          res.on('data', function(chunk){ body += chunk });
          res.on('end', function(){
              var response = JSON.parse(body);
              if(typeof response.records !== "undefined"){
                console.log(response.records.length, "events");
                data[9] = response.records.length;
              }else{ data[9] = -1;}
            fulfill(data);
          });
        }).on('error', function(e){console.log("Got an error: ", e) });
      });
  })
  .then(function(result){
      callback(null, data.join(',')+'\n');
  });

}, {parallel: 1});
inputFile.pipe(parser).pipe(transformer).pipe(outputFile);
