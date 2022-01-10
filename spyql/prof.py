import cProfile
import spyql.cli

if __name__ == "__main__":
    cProfile.run("spyql.cli.main()", "spyql.stats")
