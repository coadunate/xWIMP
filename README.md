# MICAS
> MinION Classification & Alerting System is a web application meant to be run simultaneously with the MinION DNA sequencer. This app provides an alerting system through which a scientist performing DNA sequencing runs could be notified through their email or text message. The alerts could be set to respond to any particular sequences of interest arising in their sample. Our team believes that this will enable researchers to use their time more efficiently by allowing them to focus on more important matters in the meantime, rather than waiting around for significant sequences.


## Prerequisites

- NPM & NodeJS.
- Python3 and PIP

## Installation

```sh
$ git clone https://github.com/coadunate/MICAS.git
$ cd MICAS
$ cd static
$ npm install
$ npm build
```
## Deployment

- Inside the `MICAS` directory, you need to go to `server` directory and run the
  following command to deploy the application:

```sh
$ redis-server # turn on redis background server
$ cd server
$ virtualenv -p python3 env_micas
$ source env_micas/bin/activate
$ curl https://bootstrap.pypa.io/get-pip.py | python3
$ pip install --upgrade setuptools
$ pip install -r ../requirements.txt
$ python micas.py
```

## API

Write about the function of API in this program and how it can be used.
