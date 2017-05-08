aws emr create-cluster --name "LDA_4_worker_nytimes" --ami-version 2.4.11 --service-role EMR_DefaultRole \
--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole \
--log-uri s3://s17team013.log-uri.finalproject \
--enable-debugging \
--instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=c3.xlarge InstanceGroupType=CORE,InstanceCount=4,InstanceType=c3.xlarge \
--steps Type=CUSTOM_JAR,Jar=s3://s17team013.finalproject/18645-Finalproj-1.0.2-latest.jar,Args=["-input","s3n://s17team013.finalproject/preprocess_data_from_UCI/data-nytimes/docword.nytimes.txt","-output","s3n://s17team013.output/LDA/nytimes/4/","-numOfTopic","100","-numOfIteration","1","-numOfWord","102660"] \
--auto-terminate

