import sys
import random
import operator
from pyspark import SparkConf, SparkContext

class EulerConstant:
    """
    Find Euler constant
    """

    spark_context = None

    def __init__(self):
        """
        Initialize spark context
        """
        conf = SparkConf().setAppName("Euler")
        self.spark_context = SparkContext(conf=conf)

    @staticmethod
    def counter(iter):
        count = 0
        for i in range(iter):
            sum = 0.0
            while sum < 1:
                sum += random.random()
                count += 1
        return count

    def find_euler(self, total_iterations, noof_partition=100):
        """
        Find Euler constant by iterating through partitions
        :param total_iterations:
        :param noof_partition:
        :return:
        """

        # Get total number of iterations per partition to be performed
        iteration = total_iterations/noof_partition

        total_iteration = []
        for i in xrange(noof_partition):
            total_iteration.append(iteration)

        rdd_iteration = self.spark_context.parallelize(total_iteration, noof_partition)
        rdd_counter = rdd_iteration.map(EulerConstant.counter)

        # Get total after running each
        total = rdd_counter.reduce(operator.add)

        # Get actual constant
        result = float(total)/total_iterations

        print "Total:"+str(total)
        print "Iterations:"+str(total_iterations)
        print "Euler Constant:"+ str(result)

if __name__ == "__main__":
    print(sys.argv[1])
    print(sys.argv[2])
    print(sys.argv[3])	
    print(sys.argv[4])
    total_iterations = int(sys.argv[1])
    no_of_partitions = 1
    euler = EulerConstant()
    euler.find_euler(total_iterations, no_of_partitions)
