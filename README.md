# Manifold.co

Task: identify frequently occuring apps, so we can schedule them on the same server thereby reducing latency.

How to use this Repo:

1. Run the Growth-FP notebook - runs the FP Growth algorithm on all of the data
2. Run the Rules notebook - formats the rules
3. Run the Server Assignment notebook - assigns each IP address listed in the rules (only a subset of all IPs) to a server, this also calculates the total latency time for the new model.

Methods: 

FP-Growth Algorithm
-Condensed version of Apriorir. Does not create all set lists and then assign thresholds, but builds a tree based on frequency of occurance and keeps the ones that pass your threshold.

The Apriori Algorithm notebooks were for testing, and are not in use. This method works the same as FP, but is slower.
1. Set threshold for evaluation
2. Identify frequentest itemsets that satisfy minimum thershold set above
After frequentest itemsets are identified use assocation rule mining
3. Create rules X-->Y (X = antecedent, Y = consequent)

