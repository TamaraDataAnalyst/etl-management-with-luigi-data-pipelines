#!/usr/bin/env python
# coding: utf-8

import zipfile
import luigi
import os

class Input(luigi.ExternalTask):
    """
    A zipfile.
    """

    def output(self):
        return luigi.LocalTarget("sentiment_labelled_sentences.zip")

class Unzipper(luigi.Task):
    """
    Unzips a file that contains the extracted paths. 
    """

    def requires(self):
        return Input()

    def run(self):
        """
        Extract zipped files into dirpath.
        """
        zfile = zipfile.ZipFile(self.input().path)
        for name in zfile.namelist():
            if name.endswith('.txt'):
                zfile.extract(name, '.\dest')
        zfile.close()

        # create a list of extracted paths and write them to output
        with self.output().open('w') as output:
            for root, dirs, files in os.walk('.\dest'):
                for fn in [os.path.join(root, name) for name in files]:
                    output.write("%s\n" % fn)

    def output(self):
        return luigi.LocalTarget("zip.namelist.txt")

if __name__ == '__main__':
    luigi.run()