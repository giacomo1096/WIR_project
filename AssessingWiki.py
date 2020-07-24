from gzip import GzipFile
import xml.etree.ElementTree as etree
from bs4 import BeautifulSoup as bs, NavigableString, Tag

import pandas as pd
import requests


"""
Download the list of Wikipedia Bots and store it in a Python set object. The list is retrieved from a table
available on Wikipedia. Store results in pandas dataframe
"""


print("Retrieving wikipedia bots...")
BOTS_URL = "https://en.wikipedia.org/wiki/Wikipedia:List_of_bots_by_number_of_edits"


request = requests.get(BOTS_URL)    # Download html of list of Wikipedia bots
soup = bs(request.text)             # Load html in BeautifulSoup
wiki_bots = set()                   # This set is bound to store the names of the wiki bots

# The list of bots is stored in 2 HTML tables.
# We find all tables and for each table, for each row  we store the Bot's name in the set
tables = soup.findAll("table")
for table in tables:
    if isinstance(table, NavigableString):
        continue
    else:
        rows = table.findAll("tr")
        for row in table.findAll("tr"):
            values = row.findAll("td")
            if len(values) == 3:
                wiki_bots.add(values[1].text.strip())

print("List of Wikipedia bots retrieved:")
print(wiki_bots)


"""
 Now we have a list with all bots, thus we start with the analysis of our data. We parse the 
 XML dump file and process it line by line, freeing memory as soon as we use it. Store rsults in 
 Pandas Dataframe
"""


print("Parsing xml file. It'll take a while...")
data = {} # dictionary to store the gathered info, it is later stored in a pandas Dataframe

# here we suppose we have already downloaded the dump as a Gzip archive, but it will be downloaded from within the code
#DUMP_FILE_PATH = "/Users/alessiodevoto/Desktop/Università/web_information_retrieval/project/datasets/napwiki-20200701-stub-meta-history.xml.gz"
DUMP_FILE_PATH = "dump.gz"

# we open the file ***without extracting*** the zip, otherwise it would amount to several GB
with GzipFile(DUMP_FILE_PATH) as dump_file:
    # we get an iterator in order to linearly scan the zip ?? See documentation about this ? See TestIterator.py
    context = iter(etree.iterparse(dump_file, events=('start', 'end')))
    # get root element, this is done for memory usage. If we keep it, the Element tree will create dependencies for each subelement ??

    for event, elem in context:
        if elem.tag == "{http://www.mediawiki.org/xml/export-0.10/}page" and event == "start":
            # this is a candidate, therefore we init all structures for it to be stored in final dataset
            #print("New page discovered!")
            it_is_an_article = True  # we assume it is an article, until proven otherwise

            title = None # title

            inception_date = None # first time article was inserted in Wikipedia

            inception_date_retrieved = False # bit that tells us whether inception date has been computed or not

            editor_ids = set() # empty set to store the IDS (ip or username) of editors for this article

            edits = 0 # edits count

            editors = 0 # editors count

            #elem.clear() IS THIS CORRECT?!?!?!?! TODO
        elif elem.tag == "{http://www.mediawiki.org/xml/export-0.10/}title" and event == "end":
            # we get the title and store it
            title = elem.text
            #print("with title: " + title)
            elem.clear()

        elif elem.tag == "{http://www.mediawiki.org/xml/export-0.10/}ns" and event == "end":
            # if the name space tells us the current element is not an article, we should stop the parsing and go to the next one
            # we do this by setting it_is_an_article to False and clearing the element
            if elem.text != "0":
                it_is_an_article = False
                #print("Unfortunately, it was not an article :-( I'll discard it in a second...")
            elem.clear()

        elif elem.tag == "{http://www.mediawiki.org/xml/export-0.10/}timestamp" and event == "end" and it_is_an_article and not inception_date_retrieved:
            inception_date_retrieved = True
            inception_date = elem.text[:10]
            #print("inception date:", inception_date)
            #elem.clear() TODO

        elif elem.tag == "{http://www.mediawiki.org/xml/export-0.10/}ip" and event == "end" and it_is_an_article:
            # contributor is anonymous: get the ip of the contributor
            # in this case the contributor cannot be a bot
            editor_name = elem.text
            edits = edits + 1
            editor_ids.add(editor_name)
            #print(editor_name)
            elem.clear()

        elif elem.tag == "{http://www.mediawiki.org/xml/export-0.10/}username" and event == "end" and it_is_an_article:
            # contributor is registered, hence we get his username. Could be a bot
            editor_name = elem.text
            if not editor_name in wiki_bots:
                edits = edits + 1
                editor_ids.add(editor_name)
                #print(editor_name)
            else:
                print(editor_name, "BOT DETECTED!!!\n")
            elem.clear()

        elif elem.tag == "{http://www.mediawiki.org/xml/export-0.10/}page" and event == "end" and it_is_an_article:
            # If we reach the end of a page and it_is_an article is still set to True, we add it to aout data
            #print("Adding page to dataset...")
            editors = len(editor_ids)
            data[title] = [edits, editors, inception_date, editor_ids]
            elem.clear()
            print("Page Added")

    #create pandas dataframe from dictionary, then store it in disk after having removed the editor_ids
    dataframe = pd.DataFrame.from_dict(data, orient="index",
                                       columns=['edits', 'editors', 'inception date', 'editor_ids'])
    new_df = dataframe.drop(columns = ['editor_ids'])
    new_df.to_csv("/Users/alessiodevoto/Desktop/Università/web_information_retrieval/project/assessing_wikipedia/CaWikiDataframe.csv")

"""
We now download the pageviews for each article, using the wikipedia APIs

"""

from concurrent import futures
from concurrent.futures.thread import ThreadPoolExecutor

import requests as req
import time
import pandas as pd

WEB_SERVICE_URL = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/ca.wikipedia.org/all-access/all-agents/{title}/monthly/20151001/20200101"
DUMP_FILE = "all_caWiki_names.txt"
CSV_FILE = "pageviews_all_caWiki_names.csv"


def get_next_url(file_name):
    # print("CURRENT THREAD:", threading.currentThread().getName())
    # print("Here file {file_name} starting his job".format(file_name = file_name))
    with open(file_name, "r") as file:
        for line in file:
            yield line.strip()



def get_url_views(title):
    #print("CURRENT THREAD:", threading.currentThread().getName())
    url = WEB_SERVICE_URL.format(title=title)
    print("Querying url:", url)
    http_response = None
    try:
        http_response = req.get(url)  # TODO togliamo il verify = False? TOLTO
        if http_response.status_code != 200:  # come gestiamo le pagine che vengono rifiutate?
            print("ERROR")
            print(http_response.text)
            return None
    except:
        print("An exception occured. Probably connection was refused")
        print("Let me sleep for 5 seconds")
        print("Zzzzzz...")
        time.sleep(3)
        print("Was a nice sleep, now let me continue...")
    if http_response is None:
        return None
    response = http_response.json()
    return title.replace('_', ' '), sum([x['views'] for x in response['items']])


results = []
data_res = []

with ThreadPoolExecutor(40) as executor:
    time1 = time.time()
    for url in get_next_url(DUMP_FILE):
        results.append(executor.submit(get_url_views, url))
    for future in futures.as_completed(results):
        if not future.result() is None:
            #print((future.result())[0])
            data_res.append(future.result())
    time2 = time.time()

#print(f'Took {time2 - time1:.2f} s')
dataframe = pd.DataFrame(data_res, columns=['name', 'views'])
dataframe.to_csv(CSV_FILE)




"""
At this point we have a csv file with "name", "views" and ta csv file with "name", "edits" etc...
join them.
"""

df = pd.read_csv('CaWikiDataframe.csv')
df.columns = ['name', 'edits', 'editors', 'date']
#print(df)

df_views = pd.read_csv("final_pageviews.csv")
new_df = df.set_index('name').join(df_views.set_index('name'))
#print(new_df)
new_df.to_csv("definitive_data.csv")




"""
Before statistical part, we download list of featured articles and extract the featured articles from 
the our data
"""

request = requests.get(FEATURED_URL)            # Download html of list of Wikipedia featured articles
soup = bs(request.text)                         # Load html in BeautifulSoup
high_quality_articles = set()                   # This set is bound to store the names of featured articles


# We find the table and for each row  we store the Article's name in the set
table = soup.find("table")
rows = table.findAll("tr")
#print(table)
for row in table.findAll("tr"):
    values = row.findAll("td")
    if len(values) > 3:
        #print((values[1].text).split("(")[0])
        to_add = (values[1].text).split("(")[0]
        high_quality_articles.add(to_add.strip())


print("List of Wikipedia bots retrieved:")
high_quality_articles.pop()
#print(high_quality_articles)



final_data = pd.read_csv("definitive_data.csv")
final_data.columns = ['name', 'edits', 'editors', 'date', 'id', 'views']
final_data = final_data.drop(columns = ['id'])

# empty dataframe to hold featured articles
high_quality_dataframe = pd.DataFrame(columns = ['name', 'edits', 'editors', 'date', 'id', 'views'])


#extract featured and insert into new dataframe
for index, row in final_data.iterrows():
    #print(row['name'])
    #print(row)
    if row['name'] in high_quality_articles:
        print("{name} is a high quality article!! I'll add it to new dataframe".format(name = row['name']))
        #high_quality_dataframe.append([row['name'], row['edits'], row['editors'], row['date'], row['views']])
        high_quality_dataframe = high_quality_dataframe.append(row)
        high_quality_dataframe.append(final_data.iloc[index])
        final_data.drop(index, inplace = True)



print(high_quality_dataframe)
high_quality_dataframe.to_csv("high_quality_dataframe.csv")
final_data.to_csv("filtered_final_data.csv")


"""
After the join. We have now two final datasets: high_quality_articles
"""






