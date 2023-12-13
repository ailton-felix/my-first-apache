import subprocess

if __name__=='__main__':

    subprocess.run("wget --directory-prefix=./dataset/ https://raw.githubusercontent.com/cassiobolba/Python/master/Python-Apache-Beam/voos_sample.csv", shell=True)
    print("Hello")
