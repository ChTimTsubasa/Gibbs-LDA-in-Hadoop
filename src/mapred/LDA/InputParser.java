package mapred.LDA;

/**
 * Basic input parser to parse a string into
 * int key and int array values
 * @author Tianyi Chen
 */
public class InputParser {
    int key;
    int[] val;
    public InputParser(String line) {
        String [] tokens = line.split("\\t");
        key = Integer.parseInt(tokens[0]);
        String[] items = tokens[1].replace("[", "").replace("]", "").replace(" ", "").split(",");
        val = new int[items.length];

        for (int i = 0; i < items.length; i++) {
            try {
                val[i] = Integer.parseInt(items[i]);
            } catch (NumberFormatException nfe) {
                //NOTE: write something here if you need to recover from formatting errors
            };
        }
    }
    public int[] getVal() {
        return this.val;
    }
    public int getKey() {
        return this.key;
    }
}