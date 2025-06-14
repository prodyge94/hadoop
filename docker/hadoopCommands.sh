docker exec -it namenode /bin/bash
hdfs dfs -mkdir /twitter
hdfs dfs -put ./twitter_human_bots_dataset.csv /twitter/twitter_human_bots_dataset.csv