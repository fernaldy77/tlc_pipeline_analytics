import os

### Set Environment Variables
os.environ['envn'] = 'PROD'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'

### Get Environment Variables
envn = 'PROD'

### Set Other Variables
appName="TLC Analytics"
current_path =  os.getcwd()