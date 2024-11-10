cow_alive = False
cow_env = ""
cow_workspace = ""
cow_catalog = ""


from aht_hello_dabs.main import get_taxis, sentient_cow

get_taxis()
sentient_cow()

# try: 
#     #from cowsay import cow
#     from aht_hello_dabs.main import cow_say
#     cow_alive=True
# except: 
#     cow_alive=False

# try: 
#     env = dbutils.widgets.getArgument("job_env")
#     if len(env) > 0:
#         cow_env = f"\u2713 I know that the DABS Variables are {env}"
#     else: 
#         cow_env = "x I don't know the DABS Variables"
# except: 
#     cow_env = "x I don't know the DABS Variables"

# try: 
#     from dbruntime.databricks_repl_context import get_context
#     workspaceId = get_context().workspaceId
#     cow_workspace = f"\u2713 I know that the workspace id is {workspaceId}"
# except: 
#     cow_workspace = "x I have no idea what workspace im in"

# try: 
#     current_catalog = spark.catalog.currentCatalog()
#     cow_catalog = f"\u2713 I know that I'm in the {current_catalog} catalog"
# except: 
#     cow_catalog = f"x I have no idea what catalog I'm in"

# cow_knowledge = f"{cow_env}\n{cow_workspace}\n{cow_catalog}"

# if cow_alive:
#     cow_say(cow_knowledge)
# else: 
#     print("there is no cow because he could not be imported")
#     print(cow_knowledge)