./your_program.sh sample.db .tables && printf "\n" &&
./your_program.sh sample.db .dbinfo &&  printf "\n" &&
./your_program.sh sample.db "SELECT COUNT(*) FROM apples" && printf "\n" &&
./your_program.sh sample.db "SELECT name from apples" &&  printf "\n" &&
./your_program.sh sample.db "SELECT name, color FROM apples" && printf "\n"
