from pyspark import SparkContext
import Sliding, argparse


def bfs_map(value):
    # value is a (position, level) pair

    result = [value]
    if value[1] == level:
    # if the node is at frontier
        for child in Sliding.children(WIDTH, HEIGHT, value[0]):
            result.append((child, level + 1))
    return result


    

def bfs_reduce(value1, value2):
    # given two levels, return the smaller one. This function is used to remove duplicate positions from the tree.
    if value1 < value2:
        return value1
    else:
        return value2


    

def solve_sliding_puzzle(master, output, height, width):
    """
    Solves a sliding puzzle of the provided height and width.
     master: specifies master url for the spark context
     output: function that accepts string to write to the output file
     height: height of puzzle
     width: width of puzzle
    """
    # Set up the spark context. Use this to create your RDD
    sc = SparkContext(master, "python")

    # Global constants that will be shared across all map and reduce instances.
    # You can also reference these in any helper functions you write.
    global HEIGHT, WIDTH, level

    # Initialize global constants
    HEIGHT=height
    WIDTH=width
    level = 0 # this "constant" will change, but it remains constant for every MapReduce job

    # The solution configuration for this sliding puzzle. You will begin exploring the tree from this node
    sol = Sliding.solution(WIDTH, HEIGHT)


    # Map Reduce
    tree = sc.parallelize([(sol, level)]) # all (position, level) pairs visited so far
    

    while True:
        temp = tree.flatMap(bfs_map)
        if level % 16 == 0:
            temp = temp.partitionBy(16)
        temp = temp.reduceByKey(bfs_reduce) # return a new tree that contains the children of the frontier nodes
        if level % 8 == 0:
            if temp.count() == tree.count(): # if no new positions are added, exit
                break
        tree = temp
        level = level + 1


    for s in tree.collect():
        output(str(s[1]) + " " + str(s[0]))
    sc.stop()





def main():
    """
    Parses command line arguments and runs the solver appropriately.
    If nothing is passed in, the default values are used.
    """
    parser = argparse.ArgumentParser(
            description="Returns back the entire solution tree.")
    parser.add_argument("-M", "--master", type=str, default="local[8]",
            help="url of the master for this job")
    parser.add_argument("-O", "--output", type=str, default="solution-out",
            help="name of the output file")
    parser.add_argument("-H", "--height", type=int, default=2,
            help="height of the puzzle")
    parser.add_argument("-W", "--width", type=int, default=2,
            help="width of the puzzle")
    args = parser.parse_args()


    # open file for writing and create a writer function
    output_file = open(args.output, "w")
    writer = lambda line: output_file.write(line + "\n")

    # call the puzzle solver
    solve_sliding_puzzle(args.master, writer, args.height, args.width)

    # close the output file
    output_file.close()

# begin execution if we are running this file directly
if __name__ == "__main__":
    main()
