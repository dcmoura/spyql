import cProfile
import spyql.cli

"""
Run `python3 -m spyql.prof <my_query>` for profiling. 
Profiler stats saved on file `spyql.stats`. 
"""

if __name__ == "__main__":
    cProfile.run("spyql.cli.main()", "spyql.stats")
