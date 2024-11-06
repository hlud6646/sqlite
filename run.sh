# find app/ | entr -c ./your_program.sh sample.db .tables



# find app/ | entr -c ./your_program.sh sample.db "select count(*) from apples"



find app/ | entr -c ./your_program.sh sample.db "SELECT name from apples"



