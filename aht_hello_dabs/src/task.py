
# I can easily import non DBR packages because I specified them in the environments :) 
from cowsay import cow
import os 

print("I can easily import an external python package through the use of environments")
cow("Hello, world!")

print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
import sys 
params = dbutils.widgets.getAll()

print("But I can't get job/task level params because I don't have access to dbutils")
print(params)


from dbruntime.databricks_repl_context import get_context

workspaceId = get_context().workspaceId
print("But Can I get the workspace id?")
print(workspaceId)