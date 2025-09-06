import time
import sys
import pandas as pd
from collections import defaultdict


class Yannakakis():
    def __init__(self, relations, join_tree, selection_criteria, projection_criteria, logger, logging, applyCardinalityEstimation = True):
        self.relations = relations
        self.join_tree = join_tree
        self.selection_criteria = selection_criteria
        self.projection_criteria = projection_criteria
        self.logger = logger
        self.logging = logging
        self.cardinalityEstimation = applyCardinalityEstimation
        self.timeTaken = None
        self.yannakakis(self.relations, self.join_tree ,self.selection_criteria, self.projection_criteria)
    
    def bottom_up_semi_join(self, reduced, join_tree):
        for edge in reversed(join_tree):
            '''reduced[movieinfoidx] = semijoin(reduced[movieindexleft], reduced[info_type], info_type_id, id)'''
            initialCount = len(reduced[edge["left"]])
            reduced[edge["left"]] = self.semi_join(
                reduced[edge["left"]],
                reduced[edge["right"]],
                edge["left_key"],
                edge["right_key"]
            )
            danglingTuples = initialCount - len(reduced[edge["left"]]) 
            self.logger.info(f"BOTTOM UP :- {edge}  Dangling tuples removed :-  {danglingTuples}")
        return reduced

    def top_down_semi_join(self, reduced, join_tree):
        for edge in join_tree:
            initialCount = len(reduced[edge["right"]])
            reduced[edge["right"]] = self.semi_join(
                reduced[edge["right"]],
                reduced[edge["left"]],
                edge["right_key"],
                edge["left_key"]
            )
            danglingTuples = initialCount - len(reduced[edge["right"]])
            self.logger.info(f"TOP DOWN :- {edge}  Dangling tuples removed :- {danglingTuples}")
        return reduced

    def join_phase(self, reduced, join_tree):
        """
        Alternative join approach with explicit step-by-step joining
        """
        # Track intermediate results
        intermediate = {}
        joined_result = []

        for edge in join_tree:
            # Use either existing result or the relation from reduced
            left_data = intermediate.get(edge["left"], reduced[edge["left"]])
            right_data = reduced[edge["right"]]
            # right_data = intermediate.get(edge["right"], reduced[edge["right"]])
            
            # Perform join
            joined_result = self.join(
                left_data,
                right_data,
                edge["left_key"],
                edge["right_key"]
            )
            
            # Store result for potential next joins
            intermediate[edge["right"]] = joined_result
            intermediate[edge["left"]] = joined_result
        
        # Return the final joined result
        return joined_result
    
    # Supporting Functions
    def join(self, left, right, left_key, right_key):
        """
        Perform a full join, merging rows from `left` and `right`.
        """
        if left_key not in left[0] or right_key not in right[0]:
            raise KeyError(f"Key '{left_key}' or '{right_key}' missing in relations.")
        
        # Create a dictionary for faster lookup on the right side
        # right_dict = {r[right_key]: r for r in right}
        right_dict = defaultdict(list)
        for r in right:
            right_dict[r[right_key]].append(r)
        
        result = []
        for l in left:
            matching_rows = right_dict.get(l[left_key], [])
            for r in matching_rows:
                merged_row = {**l, **r}
                result.append(merged_row)
            # if l[left_key] in right_dict:
            #     merged_row = {**l, **right_dict[l[left_key]]}  # Merge the rows where keys match O(1) lookup
            #     result.append(merged_row)
    
        return result

    def semi_join(self, left, right, left_key, right_key):
        """
        Perform semi-join without reducing columns.
        Filters rows in `left` based on matching keys in `right`.
        """
        '''reduced[movieinfoidx] = semijoin(reduced[movieindexleft], reduced[info_type], info_type_id, id)'''
        valid_keys = {row[right_key] for row in right}
        # Keep all columns from `left`
        return [row for row in left if row[left_key] in valid_keys]

    def apply_selection(self, relation, conditions):
        """
        Filter rows based on conditions.
        Each condition is a dictionary with `column`, `operator`, and `value`.
        """
        # print(f"Relation: {relation}")
        # print(f"Conditions: {conditions}")

        for condition in conditions:
            column, operator, value = condition["column"], condition["operator"], condition["value"]
            relation = [row for row in relation if pd.notna(row[column])]  # Skip rows where column value is NaN
            if operator == "==":
                relation = [row for row in relation if row[column] == value]
            elif operator == "!=":
                relation = [row for row in relation if row[column] != value]
            elif operator == ">":
                relation = [row for row in relation if row[column] > value]
            elif operator == "<":
                relation = [row for row in relation if row[column] < value]
            elif operator == "between":
                lowLimit, highLimit = value[0], value[1]
                relation = [row for row in relation if row[column] >= lowLimit and row[column] <= highLimit]
            elif operator == "like":
                res = []
                for row in relation:
                    if value.replace("%", "") in row[column]:
                        res.append(row)
                relation = res
            elif operator == "not like":
                res = []
                for row in relation:
                    if value.replace("%", "") not in row[column]:
                        res.append(row)
                relation = res
            elif operator == "IN":
                relation = [row for row in relation if row[column] in value]
            elif operator == "not in":
                relation = [row for row in relation if row[column] not in value]
        return relation

    def apply_projection(self, relation, columns):
        """
        Retain only specified columns, applying aggregations if needed.
        """
        aggregates = {}
        for col in columns:
            if col.startswith("MIN("):
                field = col[4:-1]  # Extract column name
                aggregates[col] = min(row[field] for row in relation if field in row)
            elif col.startswith("MAX("):
                field = col[4:-1]
                aggregates[col] = max(row[field] for row in relation if field in row)
            else:
                # Regular column selection
                relation = [{col: row[col] for col in columns if col in row} for row in relation]
        
        if aggregates:
            return [aggregates]  # Return single-row result for aggregations
        else:
            return relation

    def estimate_cardinality(self, relation):
        """
        Estimate the cardinality of a relation (number of rows).
        """
        return len(relation)

    def decide_join_order(self, relations, join_tree):
        """
        Decide the order of joins based on cardinality estimation.

        Args:
            relations: A dictionary of relation names and their data.
            join_tree: A list of join edges with `left`, `right`, `left_key`, `right_key`.

        Returns:
            A reordered join tree based on estimated cardinalities.
        """
        # Annotate join_tree with estimated cardinalities
        join_order = []
        for edge in join_tree:
            left_cardinality = self.estimate_cardinality(relations[edge["left"]])
            right_cardinality = self.estimate_cardinality(relations[edge["right"]])
            edge["left_cardinality"] = left_cardinality
            edge["right_cardinality"] = right_cardinality
            join_order.append(edge)

        # Sort join tree by the sum of cardinalities (smallest relations first)
        join_order.sort(key=lambda edge: edge["left_cardinality"] + edge["right_cardinality"])

        return join_order

    def calculateTimeInterval(self, prevTime, msg):
        curTime = time.time()
        timeDiff = curTime - prevTime
        self.logger.info(f"{msg} {timeDiff:.6f} seconds")
        if msg == "Overall time taken by Yannakakis algirithm :- ":
            self.timeTaken = timeDiff
        return time.time()

    def measure_memory(self, relations, phase):
        """
        Measure and print the memory usage of all relations at a given phase.
        """
        total_size = sum(sys.getsizeof(rel) for rel in relations.values())
        self.logger.info(f"{phase} - Total Memory Usage: {total_size / 1024:.2f} KB")
        for name, rel in relations.items():
            self.logger.info(f"Relation {name}: {sys.getsizeof(rel) / 1024:.2f} KB")

    def yannakakis(self, relations, join_tree, selection_criteria, projection_criteria):

        startTime = curTime = time.time()
        self.logger.info(f"Program Started at : {startTime:.6f} seconds")

        # Apply selections
        for table_name, conditions in selection_criteria.items():
            relations[table_name] = self.apply_selection(relations[table_name], conditions)

        curTime = self.calculateTimeInterval(curTime, "Time taken to Selections :- ")
        self.measure_memory(relations, "Memory Usage After Selections")

        # Decide join order based on cardinality
        if self.cardinalityEstimation:
            join_tree = self.decide_join_order(relations, join_tree)

        curTime = self.calculateTimeInterval(curTime, "Time taken to calculate join order :- ")

        # Phase 1: Bottom-Up Semi-Join Reduction
        reduced = self.bottom_up_semi_join(relations, join_tree)

        curTime = self.calculateTimeInterval(curTime, "Time taken to calculate perform bottom up semi join :- ")
        self.measure_memory(reduced, "Memory Usage After Bottom-Up Semi-Join")

        # Phase 2: Top-Down Semi-Join Reduction
        reduced = self.top_down_semi_join(reduced, join_tree)

        curTime = self.calculateTimeInterval(curTime, "Time taken to calculate perform top down semi join :- ")
        self.measure_memory(reduced, "Memory Usage After Top-Down Semi-Join")

        # Phase 3: Final Join Phase
        result = self.join_phase(reduced, join_tree)

        curTime = self.calculateTimeInterval(curTime, "Time taken to calculate perform join phase :- ")
        self.logger.info(f"Final Result - Memory Usage (Before Projection): {sys.getsizeof(result) / 1024:.2f} KB")

        # Apply projections
        # for table_name, columns in projection_criteria.items():
        #     if table_name in result[0]:  # Only apply projection to present tables
        #         result = apply_projection(result, columns)

        curTime = self.calculateTimeInterval(curTime, "Time taken to perform projections :- ")
        endTime = self.calculateTimeInterval(startTime, "Overall time taken by Yannakakis algirithm :- ")

        self.logger.info(f"Program ends at : {endTime:.6f} seconds")

        self.logger.info(f"Final Result - Memory Usage (After Projection): {sys.getsizeof(result) / 1024:.2f} KB")
        self.logger.info(f"Length of Final Join : {len(result)}")
        return result
