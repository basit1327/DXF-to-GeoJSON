'use strict';

const multer = require('multer'),
	debug = require('debug')(process.env.DEBUG_NAMESPACE),
	fs = require('fs'),
	fileUtilFunctions = require('../../utils/file_util_functions'),
	cliExecution = require('../../utils/cli_execution'),
	AWS_S3 = require('../../classes/aws_s3'),
	storageDirForDxf = './uploads/files/dxf/',
	storageDirGeoJSON = './uploads/files/geojson/',
	uniqid = require('uniqid'),
	{ StatusCodes } = require('http-status-codes');

const excludedGeometryTypes = ['Point'];

const fileStorage = multer.diskStorage({
	destination: function (req, file, cb) {
		cb(null, storageDirForDxf)
	},
	filename: function (req, file, cb) {
		let name = `${uniqid()}.dxf`
		req.savedFileName = name;
		cb(null, name);
	}
});

const uploadFileSetting = multer({
	storage: fileStorage,
	fileFilter: (req, file, callback) => {
		try{
			let extension = fileUtilFunctions.getFileExtension(file.originalname);
			if (!file || extension!== 'dxf') {
				callback('Please upload a dxf file.',null);
			} else {
				callback(null, true)
			}
		}
		catch (e){
			callback('Something went wrong in file uploading step',null)
		}
	}
}).single('designFile');

async function convertDxfToGeoJSON(req, res) {
	uploadFileSetting(req, res, async (err)=> {
		if (err) {
			res.status(StatusCodes.BAD_REQUEST).send({message:err})
		}
		else {
			try {
				if (!req.savedFileName){
					res.status(StatusCodes.BAD_REQUEST).send({message:'No file attached, Please add a .dxf file'})
				}
				else {
					let fileNameWithoutExtension = req.savedFileName.replace('.dxf','')
					let geoJSONFileName = fileNameWithoutExtension +'.geojson';

					let fileHash = await fileUtilFunctions.getFileHash(`${storageDirForDxf + req.savedFileName}`);
					if(fileHash instanceof Error){
						throw fileHash
					}

					let existAtCloud = await checkIsExistOnCloud(`${fileHash}.geojson`);
					if(!existAtCloud){
						/*===========================================================*/
						/* If file not exist at cloud then create geojson by ogr2ogr */
						//region File conversion
						let ogr2ogrCommand = `ogr2ogr 
							-f GeoJSON
							-s_srs epsg:2157 
							-t_srs epsg:4326 
							${storageDirGeoJSON + geoJSONFileName} 
							${storageDirForDxf + req.savedFileName}`;

						let cliResult = await cliExecution.executeCLI(ogr2ogrCommand.replace(/\n/g, " "), 'dxfToGeoJSON');
						if(cliResult instanceof Error){
							throw cliResult;
						}
						//endregion
					}

					//region Reading GeoJSON Data and returning layers Array
					let geoJSONData;
					if(!existAtCloud){
						/*=================================================*/
						/*  If file not exist at cloud then ogr2ogr create geojson
							and store in local file system read it from there
						 */
						geoJSONData = await fileUtilFunctions.readGeoJSONFile(`${storageDirGeoJSON + geoJSONFileName}`)
					}
					else {
						geoJSONData = JSON.parse(existAtCloud);
					}

					let layers = extractLayersFromGeoJSON(geoJSONData);
					//endregion

					//region Uploading geojson file to cloud (S3) and deleting locally stored files
					await uploadGeoJSONFileToCloud(
						existAtCloud,
						existAtCloud ? null :`${storageDirGeoJSON + geoJSONFileName}`,
							`${storageDirForDxf + req.savedFileName}`,
						fileHash
					)
					//endregion

					res.status(StatusCodes.OK)
						.send({
							message: 'DXF to GeoJSON conversion succeed',
							data: {
								fileName: `${fileHash}.geojson`,
								layers
							}
						});
				}
			}
			catch (e) {
				res.status(StatusCodes.BAD_REQUEST)
					.send({
						message: e.hasOwnProperty('message') ? e.message : 'Something went wrong',
					});
				debug(e);
			}
		}
	});
}

function extractLayersFromGeoJSON(data){
	try{
		let features = data.features;
		let layersSet = new Set();
		features.map(e=>{
			layersSet.add(e.properties.Layer)
		})
		return Array.from(layersSet);
	}
	catch (e){
		debug(e);
		throw new Error('Invalid GeoJSON data')
	}
}

async function layerFilterGeoJSONFile(req, res){
	try {
		let existAtCloud = await checkIsExistOnCloud(req.body.fileName);
		if(!existAtCloud){
			throw new Error("Sorry this file doesn't exist or removed")
		}
		let data = JSON.parse(existAtCloud);
		let features = data.features;
		let requiredLayers =  req.body.layers;
		let filteredRecord = features.filter(e=>{
			e.geometry = e.geometry || {type:''};
			e.properties = e.properties || {Layer:undefined};
			return (
				requiredLayers.indexOf(e.properties.Layer) !== -1
				&& excludedGeometryTypes.indexOf(e.geometry.type) === -1
			)
		})

		let finalData = {};
		requiredLayers.forEach(layerName=>{
			finalData[layerName] = {};
			finalData[layerName].type = 'FeatureCollection';
			finalData[layerName].features = filteredRecord.filter(record => record.properties.Layer === layerName);
		})

		data.features = filteredRecord;
		res.status(StatusCodes.OK)
			.send(finalData)
	}
	catch (e){
		res.status(StatusCodes.BAD_REQUEST)
			.send({
				message: e.hasOwnProperty('message') ? e.message : 'Something went wrong',
			});
		debug(e);
	}
}

/**
 * @description This function search the file by key at cloud (S3)
 * if not found this return false, in-case of error return Error()
 * if file found it return file data;
 * @param localFilePath
 * @returns {Promise<string|boolean>}
 */
async function checkIsExistOnCloud(fileHash){
	let readResponse = await AWS_S3.getObjectByKey(fileHash)
	if(readResponse instanceof Error){
		throw readResponse
	}
	else return readResponse;
}

/**
 * @description This function upload geojson file to cloud & after that it will
 * delete files from local storage
 * @param geojsonFile
 * @param dxfFile
 * @param fileHash
 * @returns {Promise<boolean>}
 */
async function uploadGeoJSONFileToCloud(existAtCloud, geojsonFile, dxfFile, fileHash){
	if(!existAtCloud){
		let fileStream = fs.createReadStream(geojsonFile);
		fileStream.on('error', function(err) {
			debug(err);
		});

		await AWS_S3.putObject(`${fileHash}.geojson`,fileStream);

		//deleting geojson file from local
		fs.unlink(geojsonFile, (err => {
			if (err) debug(`Failed to unlink file:${geojsonFile}`)
		}));
	}
	//deleting dxf file from local
	fs.unlink(dxfFile, (err => {
		if (err) debug(`Failed to unlink file:${dxfFile}`)
	}));
	return true;
}

module.exports = {
	convertDxfToGeoJSON,
	layerFilterGeoJSONFile
};
