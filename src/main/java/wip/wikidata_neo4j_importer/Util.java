package wip.wikidata_neo4j_importer;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

import java.io.*;

public class Util {

    /**
     * Get Buffered Reader for Compressed File (e.g. bz2 file)
     *
     * @param fileIn input file path string
     * @return
     * @throws FileNotFoundException
     * @throws CompressorException
     */
    public static BufferedReader getBufferedReaderForCompressedFile(String fileIn) throws FileNotFoundException, CompressorException {
        FileInputStream fin = new FileInputStream(fileIn);
        BufferedInputStream bis = new BufferedInputStream(fin);
        CompressorInputStream input = new CompressorStreamFactory().createCompressorInputStream(bis);
        BufferedReader br2 = new BufferedReader(new InputStreamReader(input));
        return br2;
    }

    /**
     * Find the base number of a number.
     * e.g. the base number of 200 with base 10 is 3
     *
     * @param num   input number
     * @param base  base
     * @return
     */
    public static long findBaseNumber(long num, long base) {
        if (num == 0) return 1;

        long baseNum = 0;
        while (num != 0) {
            baseNum += 1;
            num = num / base;
        }
        return baseNum;
    }

    /**
     * Add prefix to a long number.
     * e.g. if we want to add prefix 2 to an input number 31 with base 10, then we get 231
     *
     * @param num   input number
     * @param prefix    prefix to add
     * @param base  base
     * @return
     */
    public static long addPrefixToLong(long num, long prefix, long base) {
        long baseNum = findBaseNumber(num, base);
        while (baseNum > 0) {
            baseNum -= 1;
            prefix *= base;
        }
        return prefix + num;
    }
}
