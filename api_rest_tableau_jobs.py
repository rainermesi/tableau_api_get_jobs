import requests
from getpass import getpass
import json
import secrets
import sys


# ### Credentials

usr = input('Tableau usr:')
pwd = getpass('Tableau pwd:')
contentUrl = 'siteName'
site = ''
session = ''
host = 'siteUrl'
api_version = 'apiVersion'
pagesize = 100
headers = {'Accept':'application/json','Content-Type':'application/json'}


# ### API call configuration


def GernerateSession():
    global session
    session = secrets.token_urlsafe(16)  


def LogIn():

    global site

    data = '{"credentials": {"name":"'+usr+'","password":"'+pwd+'","site": {"contentUrl":"'+contentUrl+'"}}}'
    path = '/api/'+api_version+'/auth/signin'
    url = host+path

    response = requests.post(url, data=data,headers=headers, verify=False)
    print('Login code: '+str(response.status_code))
    print('Login data: '+str(response.text))

    json_data = json.loads(response.text)

    try:
        site = json_data['credentials']['site']['id']
        user = json_data['credentials']['user']['id']
        token = json_data['credentials']['token']
        
    except KeyError:
        sys.exit(1)

    TokenHeader(token)


def TokenHeader(token):
    headers['X-Tableau-Auth'] = token


def LogOut():
    path = '/api/'+api_version+'/auth/signout'
    url = host+path
    response = requests.post(url, headers=headers, verify=False)
    print('Logout code: '+str(response.status_code))


# ### Functions for querying jobs on the server


def GetPendingJobs():
    path = '/api/'+api_version+'/sites/'+site+'/jobs?filter=jobType:eq:refresh_extracts,status:eq:Pending'
    url = host+path
    
    response = requests.get(url, headers=headers, verify=False)
    json_data = json.loads(response.text)
        
    return json_data



def GetRunningJobs():
    path = '/api/'+api_version+'/sites/'+site+'/jobs?filter=jobType:eq:refresh_extracts,status:eq:InProgress'
    url = host+path
    
    response = requests.get(url, headers=headers, verify=False)
    json_data = json.loads(response.text)
        
    return json_data


def GetJobDetails(job_id):
    path = '/api/'+api_version+'/sites/'+site+'/jobs/'+job_id
    url = host+path
    
    response = requests.get(url, headers=headers, verify=False)
    json_data = json.loads(response.text)
        
    return json_data


def CancelJob(job_id):
    path = '/api/'+api_version+'/sites/'+site+'/jobs/'+job_id
    url = host+path
    
    response = requests.put(url, headers=headers, verify=False)
    json_data = json.loads(response.text)
        
    return json_data


# ### Get a list of jobs

GernerateSession()
LogIn()
payload = GetPendingJobs()
LogOut()

job_id_list = []

for i in payload['backgroundJobs']['backgroundJob']:
    job_id_list.append([i['id'], i['status']])


# ### Get a list with jobs and job details (name, type etc)


GernerateSession()
LogIn()
job_detail_list = []
for i in job_id_list:
    job_payload = GetJobDetails(i[0])
    try:
        job_detail_list.append([i[0],i[1],job_payload['job']['extractRefreshJob']['workbook']['name'],'workbook'])
    except:
        job_detail_list.append([i[0],i[1],job_payload['job']['extractRefreshJob']['datasource']['name'],'datasource'])
print(job_payload)
LogOut()


# ### Cancel a job

GernerateSession()
LogIn()
CancelJob('job_id')
LogOut()

