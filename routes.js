var express = require('express');
var router = express.Router();
var tungus = require('tungus');
var mongoose = require('mongoose');
mongoose.connect('tingodb://data_db/dataset-hotel-simulation-18-hours');
var schema = require('./schema')(mongoose);

console.log('Running mongoose version %s', mongoose.version);

router.get('/', function(req, res) {
  res.render('index', { title: 'Dataset Instrucions' });
});

router.get('/context', function(req, res) {
  res.render('context', { title: 'Hotel Simulation Context' });
});

router.get('/data', function(req, res) {
  var model = schema.model;
  model.find(function(err, records){

  	if(err){
  		console.error(err);
  		res.status(500).send(err);
  	}else{
  		console.log(records[0]);
  		res.status(200).send(records[0]);
  	}
  })
  
});

router.get('/schema', function(req, res) {
	res.status(200).json(schema.settings);
});

module.exports = router;
