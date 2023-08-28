import glob, pm4py, datetime, sys
from dateutil.parser import parse
from collections import OrderedDict
from pm4py.objects.log.importer.xes import importer as xes_importer
from datetime import timedelta
from datetime import datetime
import xml.dom.minidom



def processSensorFile(sensorsFile):
    sensorsLocations = {}
    with open(sensorsFile) as file:
        lines = file.readlines()
        lines = lines[1:] #salta intestazione
        for line in lines:
            values = line.split(',')
            sensorID = values[0]
            location = values[1].replace('\n','').strip()
            sensorsLocations[sensorID] = location
    file.close()
    return sensorsLocations




def combineLogs(userID,nDays):
    path = 'SensorLog/*.txt'
    logs = glob.glob(path)
    sensorlist = {}
    sensorvalue = ''
    subject_i = str(userID)

    for sensorlog in logs: 
        if subject_i+'_Log_motion_' in sensorlog:
            #print(sensorlog)
            sensor_name = (sensorlog.split('_')[3]).split('.txt')[0]
            with open(sensorlog, 'r') as s:
                lines = s.readlines() 
                lines = lines[1:] #salta intestazione e ultima linea vuota
                for line in lines:
                    if 'NOTHING' in line and sensorvalue!='OFF': # then sensor OFF
                        if len(line.split(' ', 1)[0].split('.')[0]) <2:
                            date = '2020-01-0' + line.split(' ', 1)[0].split('.')[0] + ' ' + line.split(' ', 1)[0].split('.')[1]
                        else:
                            date = '2020-01-' + line.split(' ', 1)[0].split('.')[0] + ' ' + line.split(' ', 1)[0].split('.')[1]
                        value = sensor_name + ' OFF'
                        #print(date + ' ' + value)
                        sensorlist[date] = value
                        sensorvalue = 'OFF'
                    elif sensorvalue!='ON':
                        if len(line.split(' ', 1)[0].split('.')[0]) <2:
                            date = '2020-01-0' + line.split(' ', 1)[0].split('.')[0] + ' ' + line.split(' ', 1)[0].split('.')[1]
                        else:
                            date = '2020-01-' + line.split(' ', 1)[0].split('.')[0] + ' ' + line.split(' ', 1)[0].split('.')[1]
                        value = sensor_name + ' ON'
                        #print(date + ' ' + value)
                        sensorlist[date] = value
                        sensorvalue = 'ON'
            sensorvalue = ''
            s.close()

    orderedSensorList = OrderedDict(sorted(sensorlist.items(), key=lambda x: parse(x[0])))

    
    # xeslog = xes_importer.apply('EventLogXES.xes')
    # activityList = {}
    # actionList = {}

    # for trace in xeslog:
    #     if str(trace[0]['Resource']) == subject_i: #subject 1
    #         activity_name = str(trace.attributes['concept:name'])
    #         if activity_name.endswith('_BP'):
    #             activity_name = activity_name[0:-3]
    #         activity_timestamp = str(trace[0]['time:timestamp'] - timedelta(days=75)).split('+')[0]
    #         activityList[activity_timestamp] = activity_name

    eventLog = open("EventLog.txt", 'r')
    activityList = {}
    actionList = {}

    for line in eventLog:
        if str(line.split(' ')[2]) == subject_i:
            if 'start' in line:
                activity_name = str(line.split(' ')[-1]).replace('\n','')
                if activity_name.endswith('_BP'):
                    activity_name = activity_name[0:-3]
                if int(line.split(' ', 1)[0].split('.')[0]) > int(nDays):
                    nDays = line.split(' ', 1)[0].split('.')[0]
                if len(line.split(' ', 1)[0].split('.')[0]) <2:
                    dateAct = '2020-01-0' + line.split(' ', 1)[0].split('.')[0] + ' ' + line.split(' ', 1)[0].split('.')[1].replace('HUMAN:','')
                else:
                    dateAct = '2020-01-' + line.split(' ', 1)[0].split('.')[0] + ' ' + line.split(' ', 1)[0].split('.')[1].replace('HUMAN:','')
                activity_timestamp = dateAct
                activityList[activity_timestamp] = activity_name
                #print(activity_name + ' ' + dateAct)


    fullEventLog = open("FullEventLog.txt", 'r')

    for line in fullEventLog:
        if 'HUMAN: '+subject_i in line:
            action_name = line.split(' ')[2]
            if int(line.split(' ', 1)[0].split('.')[0]) > int(nDays):
                nDays = line.split(' ', 1)[0].split('.')[0]
            if len(line.split(' ', 1)[0].split('.')[0]) <2:
                date = '2020-01-0' + line.split(' ', 1)[0].split('.')[0] + ' ' + line.split(' ', 1)[0].split('.')[1].replace('HUMAN:','')
            else:
                date = '2020-01-' + line.split(' ', 1)[0].split('.')[0] + ' ' + line.split(' ', 1)[0].split('.')[1].replace('HUMAN:','')
            action_timestamp = date
            actionList[action_timestamp] = action_name.replace('\n','')
        
    for sensorvalue in orderedSensorList:
        date = sensorvalue
        selected_action = '2020-01-01 00:00:00'
        for action in actionList:
            if action <= date and action>selected_action:
                selected_action = action
        orderedSensorList[sensorvalue] = orderedSensorList[sensorvalue] + ' ' + actionList[selected_action].capitalize()

    for sensorvalue in orderedSensorList:
        date = sensorvalue
        selected_act = '2020-01-01 00:00:00'
        for act in activityList:
            if act <= date and act>selected_act:
                selected_act = act
        orderedSensorList[sensorvalue] = orderedSensorList[sensorvalue] + ' ' + activityList[selected_act].capitalize()


    #orderedSensorList = OrderedDict(sorted(orderedSensorList.items(), key=lambda x: parse(x[0])))

    simlog = open('SensorLog_'+subject_i+'.txt', mode='w')

    for entry in orderedSensorList:
        #print(entry + ' ' + orderedSensorList[entry])
        simlog.write(entry + ' ' + orderedSensorList[entry] + '\n')

    #for act in activityList:
    #    print(act + ' ' + activityList[act])

    simlog.close()
    fullEventLog.close()
    eventLog.close()

    return nDays




def mergeLogs(nUsers):
    # input
    tempLog = 'SensorLog/Log_Temp.txt' #temperature measurements' log
    fullEventLog = 'FullEventLog.txt' #to get "air condition system" measurements
    tempValue = ''
    lines = []
    

    for i in range(0,int(nUsers)):
        with open('SensorLog_' + str(i) + '.txt', mode='r') as log:
            for line in log:
                lines.append(line[:-1] + ' Subject_' + str(i))

    #temperature measurements
    with open(tempLog, mode='r') as temp:
        for line3 in temp:
            if len(line3.split(' ', 1)[0].split('.')[0]) <2:
                date = '2020-01-0' + line3.split(' ', 1)[0].split('.')[0] + ' ' + line3.split(' ', 1)[0].split('.')[1]
            else:
                date = '2020-01-' + line3.split(' ', 1)[0].split('.')[0] + ' ' + line3.split(' ', 1)[0].split('.')[1]
            if line3.split(' ')[1] != tempValue:
                tempValue = line3.split(' ')[1]
                lines.append(date + ' Temp ' + tempValue[:-1])

    #air condition system measurements
    with open(fullEventLog, mode='r') as airlog:
        for line4 in airlog:
            if 'SYSTEM air_Condition' in line4:
                if len(line4.split(' ', 1)[0].split('.')[0]) <2:
                    date = '2020-01-0' + line4.split(' ', 1)[0].split('.')[0] + ' ' + line4.split(' ', 1)[0].split('.')[1].replace('SYSTEM','')
                else:
                    date = '2020-01-' + line4.split(' ', 1)[0].split('.')[0] + ' ' + line4.split(' ', 1)[0].split('.')[1].replace('SYSTEM','')
                airConditionValue = line4.split(' ')[2]
                lines.append(date + ' Air_condition_system ' + airConditionValue[:-1])

    # order by datetime
    lines.sort(key = lambda l : l.split(' ')[0] + ' ' + l.split(' ')[1])

    # output
    completeLog = open('CompleteLog.txt', mode='w')
    for entry in lines:
        #print(entry)
        completeLog.write(entry + '\n')


    log.close()
    temp.close()
    airlog.close()
    completeLog.close()




def convertLog(nUsers,nDays,sensorsLocations):
    inputLog = 'CompleteLog.txt'
    outputXML = open("parsedLog.xml", mode='w')

    eventId = 1
    obsId = 1
    
    output = ''
    output = '<EventLog><ObjectList>'
    objectId = 0
    #define each user as an object
    for i in range(0,int(nUsers)): 
        output = output + '<Object id=\"objID_'+str(objectId)+'\" type= "FeatureOfInterest">'
        output = output + '<attribute id=\"objectAttrID_1\" name=\"Name\" initValue=\"User'+str(i)+'\" /></Object>'
        objectId = objectId+1
    
    # define each day of simulation as an object
    dayDate = datetime.strptime('2020-01-01', "%Y-%m-%d")
    days = {}
    for j in range(int(nUsers),int(nUsers)+int(nDays)): #define subjects as objects
        output = output + '<Object id=\"objID_'+str(objectId)+'\" type= "FeatureOfInterest">'
        output = output + '<attribute id=\"objectAttrID_1\" name=\"Day_'+str(j-int(nUsers)+1)+'\" initValue=\"'+datetime.strftime(dayDate, "%Y-%m-%d")+'\" /></Object>'
        days[datetime.strftime(dayDate, "%Y-%m-%d")] = str(j)
        dayDate = dayDate + timedelta(days=1)
        objectId = objectId+1

    #define list of locations
    distinctLocations = set()
    for k in sensorsLocations.keys():
        if sensorsLocations[k] not in distinctLocations:
            output = output + '<Object id=\"objID_'+str(objectId)+'\" type= "FeatureOfInterest">'
            output = output + '<attribute id=\"objectAttrID_1\" name=\"Location\" initValue=\"'+sensorsLocations[k].strip()+'\" /></Object>'
            distinctLocations.add(sensorsLocations[k])
            #print(distinctLocations)
            objectId = objectId+1
        sensorsLocations[k] = str(objectId-1)
    # House location
    output = output + '<Object id=\"objID_'+str(objectId)+'\" type= "FeatureOfInterest">'
    output = output + '<attribute id=\"objectAttrID_1\" name=\"Location\" initValue=\"house\" /></Object>'
    sensorsLocations['house'] = str(objectId)
    output = output + '</ObjectList>'

    # define list of sensors
    output = output + '<DataSourceList>'
    for s in sensorsLocations.keys():
        if 'temp' in s.lower(): # temperature sensor
            output = output + '<DataSource id=\"' + str(s) + '\" type= "Sensor" observableProperty="temperature" />'
        elif 'house' in s.lower(): #skip
            continue
        else: #motion sensor
            output = output + '<DataSource id=\"' + str(s) + '\" type= "Sensor" observableProperty="motion" />'
    # insert PAIS
    output = output + '<DataSource id="pais_1" type= "PAIS" digitalProperty= "" />'
    output = output + '</DataSourceList><EventList>'

    # data structures for each user
    l = locals()
    for user in range(0,int(nUsers)):
        l['currentActivity_subject{}'.format(user)] = ''
        l['itAnalysesEventsPEListIDs_subject{}'.format(user)] = []
        l['currentAction_subject{}'.format(user)] = ''
        l['itAnalysesEventsListIDs_subject{}'.format(user)] = []
        l['lastLocation_subject{}'.format(user)] = ''
    
    #currentActivity_subject1=''
    #currentActivity_subject2=''
    #itAnalysesEventsPEListIDs_subject1 = []
    #itAnalysesEventsPEListIDs_subject2 = []
    #currentAction_subject1=''
    #currentAction_subject2=''
    #itAnalysesEventsListIDs_subject1 = []
    #itAnalysesEventsListIDs_subject2 = []
    
    previousTimestamp = ''

    with open(inputLog, 'r') as log:
        lines = log.readlines()
        for line in lines:
            if 'Temp' in line: #IoT Event (temperature changes)
                iotEventXES = '<IoTEvent id=\"eventID_' + str(eventId) + '\" timestamp=\"' + line.split(' ')[0] + 'T' + line.split(' ')[1] + '\"'
                caseID = '' # caseID = ALL users + day + location
                for i in range(0,int(nUsers)):
                    caseID = caseID + 'objID_' + str(i) + ','
                caseID = caseID + 'objID_' + days[line.split(' ')[0]] # day
                caseID = caseID + ',objID_' + sensorsLocations['Temp'] # location
                iotEventXES = iotEventXES + ' caseID=\"' + caseID + '\">'
                iotEventXES = iotEventXES + '<Observation id=\"obs_'+str(obsId)+'\" resultTime=\"'+ line.split(' ')[0] + 'T' + line.split(' ')[1] +'\" value=\"'+line.split(' ')[3].strip()+'\"'
                iotEventXES = iotEventXES + ' sensor=\"'+line.split(' ')[2]+'\" featureOfInterest=\"objID_'+ sensorsLocations['Temp'] +'\" /></IoTEvent>'
                output = output + iotEventXES
                eventId = eventId+1
                obsId = obsId+1
            elif 'Air_condition_system' in line: # Context Event (air condition system)
                contextEventXES = '<ContextEvent id=\"eventID_' + str(eventId) + '\" timestamp=\"' + line.split(' ')[0] + 'T' + line.split(' ')[1] + '\"'
                caseID = '' # caseID = location + day
                caseID = caseID + 'objID_' + sensorsLocations['house'] # location = entire environment
                caseID = caseID + ',objID_' + days[line.split(' ')[0]] # day
                contextEventXES = contextEventXES + ' caseID=\"' + caseID + '\">'
                contextEventXES = contextEventXES + '<ContextVariable attibuteID=\"' + line.split(' ')[2] + '\" newValue=\"' +line.split(' ')[3]+ '\" />'
                contextEventXES = contextEventXES + '<ISDataEntry storedIn=\"pais_1\" timestamp=\"' + line.split(' ')[0] + 'T' + line.split(' ')[1] + '\"'
                contextEventXES = contextEventXES + ' label=\"' + line.split(' ')[2] + '\" value=\"' +line.split(' ')[3]+ '\" />'            
                contextEventXES = contextEventXES + '</ContextEvent>'
                output = output + contextEventXES
                eventId = eventId+1
                obsId = obsId+1
            else:
                # IoT Event (low level)
                caseID='' # user + day + location
                iotEventXES = '<IoTEvent id=\"eventID_' + str(eventId) + '\" timestamp=\"' + line.split(' ')[0] + 'T' + line.split(' ')[1] + '\"'
                if 'Subject_' in line:
                    subjectId = line[-2:-1]
                    caseID = 'objID_' + subjectId # user
                    l['itAnalysesEventsListIDs_subject{}'.format(subjectId)].append(eventId)
                    l['lastLocation_subject{}'.format(user)] = sensorsLocations[line.split(' ')[2]]
                caseID = caseID + ',objID_' + days[line.split(' ')[0]] #day
                caseID = caseID + ',objID_' + sensorsLocations[line.split(' ')[2]] # location
                iotEventXES = iotEventXES + ' caseID=\"'+caseID+'\">'
                iotEventXES = iotEventXES + '<Observation id=\"obs_'+str(obsId)+'\" resultTime=\"'+ line.split(' ')[0] + 'T' + line.split(' ')[1] +'\" value=\"'+line.split(' ')[3]+'\"'
                iotEventXES = iotEventXES + ' sensor=\"'+line.split(' ')[2]+'\" featureOfInterest=\"objID_'+ sensorsLocations[line.split(' ')[2]] +'\" /></IoTEvent>'
                output = output + iotEventXES
                eventId = eventId+1
                obsId = obsId+1


                # IoT Event (middle level)
                if 'Subject_' in line:
                    subjectId = line[-2:-1]
                    if l['currentAction_subject{}'.format(subjectId)] == '':
                        l['currentAction_subject{}'.format(subjectId)] = line.split(' ')[4]
                    elif l['currentAction_subject{}'.format(subjectId)] != line.split(' ')[4]:
                        itAnalysesString = ''
                        for e in l['itAnalysesEventsListIDs_subject{}'.format(subjectId)]:
                            itAnalysesString = itAnalysesString+'eventID_'+str(e)+','
                        l['itAnalysesEventsListIDs_subject{}'.format(subjectId)] = []
                        caseID='' # user + day
                        caseID = 'objID_' + subjectId # user
                        caseID = caseID + ',objID_' + days[line.split(' ')[0]] #day
                        iotEventXES = '<IoTEvent id=\"eventID_' + str(eventId) + '\" timestamp=\"' + line.split(' ')[0] + 'T' + line.split(' ')[1] + '\"'
                        iotEventXES = iotEventXES + ' caseID=\"'+caseID+'\"><Observation id=\"obs_'+str(obsId)+'\" resultTime=\"'+ line.split(' ')[0] + 'T' + line.split(' ')[1] +'\" value=\"'+l['currentAction_subject{}'.format(subjectId)]+'\"'
                        iotEventXES = iotEventXES + ' featureOfInterest=\"objID_'+l['lastLocation_subject{}'.format(user)]+'\"/>'
                        iotEventXES = iotEventXES + '<Analytics itGenerates=\"eventID_'+str(eventId)+'\" itAnalysesEvents=\"'+itAnalysesString[:-1]+'\">'
                        iotEventXES = iotEventXES + '<Methods><Method name="CEP"></Method></Methods></Analytics>'
                        iotEventXES = iotEventXES + '</IoTEvent>'
                        output = output + iotEventXES
                        l['itAnalysesEventsPEListIDs_subject{}'.format(subjectId)].append(eventId)
                        eventId = eventId+1
                        obsId = obsId+1
                        l['currentAction_subject{}'.format(subjectId)] = line.split(' ')[4]
                

                # Process Event (High level)
                if 'Subject_' in line:
                    subjectId = line[-2:-1]
                    if l['currentActivity_subject{}'.format(subjectId)] == '':
                        l['currentActivity_subject{}'.format(subjectId)] = line.split(' ')[5]
                    elif l['currentActivity_subject{}'.format(subjectId)] != line.split(' ')[5]:
                        itAnalysesString = ''
                        for e in l['itAnalysesEventsPEListIDs_subject{}'.format(subjectId)]:
                            itAnalysesString = itAnalysesString+'eventID_'+str(e)+','
                        l['itAnalysesEventsPEListIDs_subject{}'.format(subjectId)] = []
                        caseID='' # user + day
                        caseID = 'objID_' + subjectId # user
                        caseID = caseID + ',objID_' + days[line.split(' ')[0]] #day
                        ProcessEventXES = '<ProcessEvent id=\"eventID_' + str(eventId) + '\" timestamp=\"' + line.split(' ')[0] + 'T' + line.split(' ')[1] + '\"'
                        ProcessEventXES = ProcessEventXES + ' caseID=\"'+caseID+'\"><Activity value=\"'+l['currentActivity_subject{}'.format(subjectId)]+'\" />'
                        ProcessEventXES = ProcessEventXES + '<LifecyclePhase value="complete" />'
                        ProcessEventXES = ProcessEventXES + '<Analytics itGenerates=\"eventID_'+str(eventId)+'\" itAnalysesEvents=\"'+itAnalysesString[:-1]+'\">'
                        ProcessEventXES = ProcessEventXES + '<Methods><Method name="CEP"></Method></Methods></Analytics>'
                        ProcessEventXES = ProcessEventXES + '</ProcessEvent>'
                        output = output + ProcessEventXES
                        eventId = eventId+1
                        obsId = obsId+1
                        l['currentActivity_subject{}'.format(subjectId)] = line.split(' ')[5]
                

    output = output + '</EventList></EventLog>'


    # TO CONVERT TO "NICE" XML WITH INDENTATION
    dom = xml.dom.minidom.parseString(output)
    pretty_xml_as_string = dom.toprettyxml()
    outputXML.write(pretty_xml_as_string)

    log.close()
    outputXML.close()




##########################
###### MAIN PROGRAM ######
##########################

nUsers = sys.argv[1] # number of users in the environment
nDays = 0 # keep track of how many days of simulation are available in the log

# step 0 - process sensors' locations
sensorsLocations = processSensorFile(sys.argv[2])

# step 1 - combine all logs and obtain a sublog for each user
for i in range(0,int(nUsers)):
    nDays = combineLogs(i,nDays)

# step 2 - merge all sublogs into a single complete log
mergeLogs(nUsers)

#step 3 - convert the complete log to the XML format
convertLog(nUsers,nDays,sensorsLocations)