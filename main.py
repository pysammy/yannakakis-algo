from yannakakis.db import Database
from yannakakis.yannakakis import Yannakakis
from yannakakis.jobdataset.JobQuery1A import JobQuery1A
from yannakakis.jobdataset.JobQuery5C import JobQuery5C
from yannakakis.jobdataset.JobQuery5B import JobQuery5B
import logging
import itertools


def generate_join_tree_permutations(join_tree):
    """
    Generate all permutations of the join tree, ensuring minimal connectivity.
    
    Args:
    join_tree (list): List of dictionaries representing joins between tables
    
    Returns:
    list: List of all join tree permutations
    """
    def is_valid_permutation(perm):
        """
        Check if a permutation maintains minimal table connections.
        
        Args:
        perm (list): A permutation of join tree entries
        
        Returns:
        bool: True if the permutation is valid, False otherwise
        """
        # Track unique tables in the permutation
        all_tables = set()
        for join in perm:
            all_tables.add(join['left'])
            all_tables.add(join['right'])
        
        # Check if we have at least one connection between joins
        for i in range(len(perm) - 1):
            for j in range(i + 1, len(perm)):
                # If any two joins share a table, consider it a valid permutation
                if (perm[i]['left'] == perm[j]['left'] or 
                    perm[i]['left'] == perm[j]['right'] or 
                    perm[i]['right'] == perm[j]['left'] or 
                    perm[i]['right'] == perm[j]['right']):
                    return True
        
        return False

    # Generate all permutations
    valid_permutations = []
    for perm in itertools.permutations(join_tree):
        # Convert permutation to list
        perm_list = list(perm)
        
        # Check if this permutation has at least one table connection
        if is_valid_permutation(perm_list):
            valid_permutations.append(perm_list)
    
    return valid_permutations

try:

    # Clean the log file first 
    with open("yannakakis.log", "w") as file:
        pass 

    # Set the log details
    logging.basicConfig(
        filename="yannakakis.log",
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logger = logging.getLogger(__name__)

    # Connect to the database and load the job dataset params
    db = Database()
    job = JobQuery5C()

    # Run Yannakakis
    logger.setLevel(logging.INFO)
    permutations = generate_join_tree_permutations(job.join_tree)
    minTime = float('inf')
    bestCombination = job.join_tree
    maxTime = float('-inf')
    worstCombination = job.join_tree
    for perm in permutations:
        tables = {
        table_name: db.fetch_table_from_db(table_name, columns, db.connection)
        for table_name, columns in job.columns.items()
        }
        logger.info("================================================================================================")
        logger.info(f"Executing for join tree :-  {perm} ")
        yannakakis = Yannakakis(tables, perm, job.selection_criteria, job.projection_criteria, logger, logging, False)
        logger.info("================================================================================================")
        if minTime > yannakakis.timeTaken:
            minTime = yannakakis.timeTaken
            bestCombination = perm
        
        if maxTime < yannakakis.timeTaken:
            maxTime = yannakakis.timeTaken
            worstCombination = perm
    
    logger.info(f"overall best combination was : {bestCombination}")
    logger.info(f"overall best time taken was: {minTime}")

    logger.info(f"overall worst combination was : {worstCombination}")
    logger.info(f"overall worst time taken was: {maxTime}")

except Exception as e:
    logger.setLevel(logging.ERROR)
    logger.error(f"Error occurred :  {e}")
finally:
    db.closeConnection()
