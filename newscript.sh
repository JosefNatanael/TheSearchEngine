SERVICE_NAME=searchengine_service
PATH_TO_JAR=/home/sed/project/searchengine-0.0.1-SNAPSHOT.jar
PID_PATH_NAME=/tmp/searchengine_service-pid

isIndex=n
fromScratch=n
declare -i minNumPages=-1

case $1 in
start)
        echo "Starting $SERVICE_NAME ..."
        printf "Welcome to Bolang the Web Explorer!\n\n"
        printf "To crawl or not to crawl? (c/n)\n"
        
        read isIndex
        
        if [ $isIndex == "c" ]
        then
                printf "From scratch or not from scratch? (s/n)\n"
                read fromScratch
        
                printf "Enter minimum number of pages you wish to index: (-1 for default)\n"
                read minNumPages
        
        fi
  if [ ! -f $PID_PATH_NAME ]; then
        nohup java -jar $PATH_TO_JAR $isIndex $fromScratch $minNumPages > output.txt &
        echo $! > $PID_PATH_NAME
        echo "$SERVICE_NAME started ..."
	echo "$isIndex"
	echo "$fromScratch"
	echo "$minNumPages"
  else
        echo "$SERVICE_NAME is already running ..."
  fi
;;
stop)
  if [ -f $PID_PATH_NAME ]; then
         PID=$(cat $PID_PATH_NAME);
         echo "$SERVICE_NAME stoping ..."
         kill $PID;
         echo "$SERVICE_NAME stopped ..."
         rm $PID_PATH_NAME
  else
         echo "$SERVICE_NAME is not running ..."
  fi
;;
restart)
  if [ -f $PID_PATH_NAME ]; then
      PID=$(cat $PID_PATH_NAME);
      echo "$SERVICE_NAME stopping ...";
      kill $PID;
      echo "$SERVICE_NAME stopped ...";
      rm $PID_PATH_NAME
      echo "$SERVICE_NAME starting ..."
      nohup java -jar $PATH_TO_JAR /tmp 2>> /dev/null >> /dev/null &
      echo $! > $PID_PATH_NAME
      echo "$SERVICE_NAME started ..."
  else
      echo "$SERVICE_NAME is not running ..."
     fi     ;;
 esac

