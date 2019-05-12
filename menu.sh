#!/bin/bash
# Bash Menu Script Example

PS3='Choice: '
options=("HDFS Menu" "MapReduce Tutorial" "Quit")
hdfsoptions=("Get File Status" "Make Directory" "Create File" "Read File" "Delete File" "Copy File" "Quit")
mapreoptions=("Average" "Max-Min" "Range" "Spread" "Sum" "Quit")

select opt in "${options[@]}"
do
    case $opt in
        "HDFS Menu")
            echo "you chose choice 1"
            select hdfsopt in "${hdfsoptions[@]}"
            do
                case $hdfsopt in
                    "Get File Status")
						printf "File name : "
						read -r filename
                        echo "you chose choice $filename"
                        ;;
                    "Make Directory")
						printf "Directory Name : "
						read -r dirname
                        echo "you chose choice 2"
                        ;;
					"Create File")
                        printf "File name : "
						read -r filename
                        echo "you chose choice $filename"
                        ;;
					"Read File")
                        printf "File name : "
						read -r filename
                        echo "you chose choice $filename"
                        ;;
					"Delete File")
                        printf "File name : "
						read -r filename
                        echo "you chose choice $filename"
                        ;;
					"Copy File")
						printf "Source : "
						read -r filesource
						printf "Destination : "
						read -r filedestination
                        echo "you chose choice $filesource $filedestination"
                        ;;	
                    "Quit")
						echo "1) HDFS Menu"
						echo "2) MapReduce Tutorial"
						echo "3) Quit"
                        break
                        ;;
                    *) echo "invalid option $REPLY";;
                esac
				echo "-------------------"
				echo "1) Get File Status" 
				echo "2) Make Directory" 
				echo "3) Create File" 
				echo "4) Read File" 
				echo "5) Delete File" 
				echo "6) Copy File" 
				echo "7) Back Main Menu"
				echo "------------------"
            done
            ;;
        "MapReduce Tutorial")
            echo "you chose choice 2"

            select mapreopt in "${mapreoptions[@]}"
            do
                case $mapreopt in
                    "Average")
						printf "input : "
						read -r input
						printf "output : "
						read -r output
                        echo "you chose choice $input $output"
                        ;;
                    "Max-Min")
						printf "input : "
						read -r input
						printf "output : "
						read -r output
                        echo "you chose choice $input $output"
                        ;;
                    "Range")
						printf "input : "
						read -r input
						printf "output : "
						read -r output					
                        echo "you chose choice $input $output"
                        ;;
                    "Spread")
						printf "input : "
						read -r input
						printf "output : "
						read -r output					
                        echo "you chose choice $input $output"
                        ;;
                    "Sum")
						printf "input : "
						read -r input
						printf "output : "
						read -r output					
                        echo "you chose choice $input $output"
                        ;;
                    "Quit")
						echo "---------------------"
                        echo "1) HDFS Menu"
                        echo "2) MapReduce Tutorial"
                        echo "3) Quit"
						echo "---------------------"
                        break
                        ;;
                    *) echo "invalid option $REPLY";;
                esac
				echo ""
				echo "-------------------"
				echo "1) Average" 
				echo "2) Max-Min" 
				echo "3) Range" 
				echo "4) Spread" 
				echo "5) Sum" 
				echo "6) Back Main Menu"
				echo "------------------"
            done
			
            ;;
        "Quit")
            break
            ;;
        *) echo "invalid option $REPLY";;
    esac
done
