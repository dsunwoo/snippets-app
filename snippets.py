import psycopg2
import logging
import argparse
# Set the log output file, and the log level
logging.basicConfig(filename="snippets.log", level=logging.DEBUG)

# Connect to database
logging.debug("Connecting to PostgreSQL")
connection=psycopg2.connect(database="snippets")
logging.debug("Database connection established")

def put(name, snippet, hide, show):
    """Store a snippet with an associated name.
    
    Returns the name and the snippet
    """
    if hide:
        hide_type="hidden"
    else:
        hide_type="visible"
        
    logging.info("Storing {!r} snippet {!r}: {!r}".format(hide_type, name, snippet))
    with connection, connection.cursor() as cursor:
        try:
            cursor.execute("insert into snippets values (%s, %s, %s)"
                ,(name, snippet, hide))
        except psycopg2.IntegrityError as e:
            connection.rollback()
            command="update snippets set message=%s, hidden=%s where keyword=%s"
            cursor.execute(command, (snippet, hide, name))
    connection.commit()
    logging.debug("Snippet stored successfully.")
    return name, snippet, hide
    
def delete(name):
    """Delete a snippet entry with a given name."""
    logging.info("Deleting snippet {!r}".format(name))
    with connection, connection.cursor() as cursor:
        # row=cursor.fetchone()
        cursor.execute("delete from snippets where keyword=%s", (name,))
    connection.commit()
    logging.debug("Message for Snippet Keyword {0} deleted successfully"
        .format(name))
    return name

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
        cursor.execute("select keyword, hidden from snippets where not hidden order by keyword")
        rows=cursor.fetchall()
        print('Total: ',cursor.rowcount)
        for row in rows:
            print("\tKeyword: ",row[0],"\tHidden: ",row[1])
    connection.commit()
    logging.debug("Message for Catalog: All keywords retrieved successfully")
    return ""

def search(mkey):
    """Perform a serach on the message field for string provided"""
    logging.info("Performing search function on message field")
    with connection, connection.cursor() as cursor:
        cursor.execute("select * from snippets where not hidden and message like %s"
            ,('%'+mkey+'%',))
        rows=cursor.fetchall()
        print('Total: ',cursor.rowcount)
        print("\tKeyword:\t\tMessage:\t\tHidden:\n")
        for row in rows:
            print("\t",row[0],"\t\t",row[1],"\t\t",row[2])
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
    put_parser.add_argument("--hide", help="Hide the row from view",
                            action="store_true", default=False)
    put_parser.add_argument("--show","--unhide","--no-hide","--hide=0",
                            help="Make the row visible (default)",
                            action="store_true", default=True)
                            
    # Subparser for the delete command
    logging.debug("Constructing delete subparser")
    get_parser = subparsers.add_parser("delete", help="Delete a snippet")
    get_parser.add_argument("name", help="Delete a keyword entry in snippets DB")
    
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

    # Handle the optional Hide/Show arguments
    if arguments.command == 'put':
        if arguments.show:
            arguments.hide = False
        else:
            arguments.hide = True
        if arguments.hide:
            arguments.show = False
        else:
            arguments.show = True
        
    # Convert parsed arguments from Namespace to dictionary
    arguments = vars(arguments)
    command = arguments.pop("command")

    if command == "put":
        name, snippet, hide = put(**arguments)
        print("Stored {!r} as {!r}. Column hidden = {!r}".format(snippet, name, hide))
    elif command == "delete":
        name = delete(**arguments)
        print("Deleted {!r} from the snippets database".format(name))
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