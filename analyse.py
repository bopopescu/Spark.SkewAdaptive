#!/usr/bin/python

print "analyse spark log"

logfile = open("./spark-log.log",'r')
report = open("./analyse.log",'w')
temp=[]
temp1=[]

for line in logfile:
	if "@@@@" in line:
		temp.append(line)
	if "Running task " in line:
		temp.append(line)
	if "Got assigned task" in line:
		temp.append(line)
	if "Finished task" in line:
		temp.append(line)
	if "Final stage" in line:
		temp.append(line)
	if "Parents of final stage" in line:
		temp.append(line)
	if "Missing parents" in line:
		temp.append(line)
	if "missing tasks" in line:
		temp.append(line)
	#if "parentName:" in line:
	#	temp.append(line)

for x in temp:
	temp1.append(x.replace(" [Logging$class:logInfo:59]","",1))

report.writelines(temp1)

logfile.close()
report.close()


