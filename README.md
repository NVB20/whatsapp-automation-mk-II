# whatsapp-automation-mk-II
whatsapp automation using Selenium, PostgreSQL and mcp

# improvments:
 selenium driver now have session persistence fallback if not read messages and runs fasster 

 structure:
 -
 --src
 ---etl
 ---- extract.py --> selenium whatsapp read
 ---- transform.py --> clean message, filter into 2 dicts(practice/messages), connect lesson by id ---> jsonify
 ---- load.py --> insert into mongoDB || create studentsDB and messagesDB
 - .gitignore
 - secrets