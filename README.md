imbo-image-transfer
===================

Copies images from one Imbo-installation to another installation or user

## Requirements

* Node.js >= 0.10

## Installation/usage

* Clone/download the repository
* Run `npm install` to install dependencies
* Copy `config.json.dist` to `config.json` and alter the values to fit your use
* Run it using `node transfer-images.js`

Should you want to only fetch images since a given date,
use `--since <unix-timestamp-in-milliseconds>`

You also have the option to make it a little more verbose by using `--verbose`
