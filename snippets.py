import psycopg2
import logging
import argparse
# Set the log output file, and the log level
logging.basicConfig(filename="snippets.log", level=logging.DEBUG)

# Connect to database
logging.debug("Connecting to PostgreSQL")
connection=psycopg2.connect(database="snippets")
logging.debug("Database connection established")


def put(name, snippet):
    """Store a snippet with an associated name.
    
    Returns the name and the snippet
    """
    logging.info("Storing snippet {!r}: {!r}".format(name, snippet))
    with connection, connection.cursor() as cursor:
        try:
            cursor.execute("insert into snippets values (%s, %s)", (name, snippet))
        except psycopg2.IntegrityError as e:
            connection.rollback()
            command="update snippets set message=%s where keyword=%s"
            cursor.execute(command, (snippet, name))
    connection.commit()
    logging.debug("Snippet stored successfully.")
    return name, snippet
    
def get(name):
    """Retrieve the snippet with a given name.
    
    If there is no such snippet, return '404: Snippet Not Found'.
    
    Returns the snippet.
    """
    logging.info("Retrieving snippet {!r}".format(name))
    with connection, connection.cursor() as cursor:
        cursor.execute("select message from snippets where keyword=%s", (name,))
        row=cursor.fetchone()
    connection.commit()
    logging.debug("Message for Snippet Keyword {0} retrieved successfully"
        .format(name))
    if not row:
        # No snippet was found with the keyword supplied
        return "404: Snippet not Found"
    return row[0]

def catalog():
    """Retrieve a list of keywords in the database for user to view and choose"""
    logging.info("Retrieving all snippet keywords")
    with connection, connection.cursor() as cursor:
        cursor.execute("select keyword from snippets order by keyword")
        rows=cursor.fetchall()
        print('Total: ',cursor.rowcount)
        for row in rows:
            print("\t",row[0])
    connection.commit()
    logging.debug("Message for Catalog: All keywords retrieved successfully")
    return ""

def search(mkey):
    """Perform a serach on the message field for string provided"""
    logging.info("Performing search function on message field")
    with connection, connection.cursor() as cursor:
        cursor.execute("select * from snippets where message like %s",(mkey,))
        rows=cursor.fetchall()
        print('Total: ',cursor.rowcount)
        print("\tKeyword \tMessage\n")
        for row in rows:
            print("\t",row[0],"\t",row[1])
    connection.commit()
    logging.debug("Message for Search: Successfully Executed")
    return mkey
    
def main():
    """Main function"""
    logging.info("Constructing parser")
    parser = argparse.ArgumentParser(description="Store and retrieve snippets of text")
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Subparser for the put command
    logging.debug("Constructing put subparser")
    put_parser = subparsers.add_parser("put", help="Store a snippet")
    put_parser.add_argument("name", help="Name of the snippet")
    put_parser.add_argument("snippet", help="Snippet text")
    
    # Subparser for the get command
    logging.debug("Constructing get subparser")
    get_parser = subparsers.add_parser("get", help="Retrieve a snippet")
    get_parser.add_argument("name", help="Keyword in snippets DB")
    
    # Subparser for the catalog command
    logging.debug("Constructing catalog subparser")
    get_parser = subparsers.add_parser("catalog", help="Lists all keywords in DB")
    
     # Subparser for the search command
    logging.debug("Constructing search subparser")
    get_parser = subparsers.add_parser("search", help="Search message field for string")
    get_parser.add_argument("mkey", help="search string for message field")
    
    arguments = parser.parse_args()
   
    # Convert parsed arguments from Namespace to dictionary
    arguments = vars(arguments)
    command = arguments.pop("command")

    if command == "put":
        name, snippet = put(**arguments)
        print("Stored {!r} as {!r}".format(snippet, name))
    elif command == "get":
        snippet = get(**arguments)
        print("Retrieved snippet: {!r}".format(snippet))
    elif command == "catalog":
        print("Retrieved a list of keywords")
        snippet = catalog()
    elif command == "search":
        snippet = search(**arguments)
        print("The string {!r} was found in the above message fields:\n".format(snippet))
        
if __name__ == "__main__":
    main()