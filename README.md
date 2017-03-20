# AirportDelay
Ever wonder why flight delays happen?  When is the best time of day/day of week/time of year to fly to minimise delays? Well this assignment aims to test your skills that can help build apps to answer such type of question provided the data.

Program Running Instructions :

All the input fields are hardCodes 

Airport Year Data
a. hdfs://10.2.2.28:8020/airportInput

Airport Name and Airport code

b. hdfs://10.2.2.28:8020/airportList

Carrier Codes and Fully Qualified Names
c. hdfs://10.2.2.28:8020/carrierList

cd spark-1.6.3-bin-hadoop2.6/bin

./spark-submit --class com.cloudwick.practise.AirportDelay --master spark://rajiv:7077 /root/Airport-1.0-SNAPSHOT.jar 


