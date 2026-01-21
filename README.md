# MIMIC-IV-ETL-Pipeline
This repository features all the code and documentation for my personal project using real MIMIC-IV medical data. AWS will be the cloud service I use to run the ETL pipeline to clean and store the messy MIMIC-IV data.


# Tech Stack
I am using Python, SQL and AWS for this end-to-end pipeline. This project allows me to get hands on experience working with AWS features such as
S3 and RDS. I am experienced in Python and I understand SQL queries, so combining those two skills with my newly found interest in cloud services is a rewarding crossover.


# Motivation / Problem to Solve
Messy healthcare data is everywhere. I want to gain valuable hands on knowledge for proper extraction, meaningful transformation of the data and effective loading so that the data is usable
downstream (Power BI, Tableua, etc.). This is a test in not only being able to orchestrate the different pieces of the ETL pipeline but to design a system that uses the tools and knowledge I have now to cleanly ingest messy, real world data. Challenging myself to build something robust with softwares that I am not 100% familiar with is the type of work that will increase my 
data engineering skillset very quickly in a short amount of time.


# MIMIC-IV Dataset
The MIMIC-IV dataset is a very well known and large dataset that contains hospital and ICU stay information. This information is extremely private as it can contain identifiers to 
patients, so it was a bit of a mess to jump through the hoops to get access to it. If you are looking to gain access to the dataset yourself, you need to register on Physionet.org and complete
a credentialing, apply for the dataset using a reference (my academic advisor, personally) and also complete CITI training modules with proof of passing. 
After gaining access to the dataset, you can either fully download the files (~10 GB) or you can download the specific .csv files you want directly to a S3 bucket utilizing the AWS CLI and an IAM account.
