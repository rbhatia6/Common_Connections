import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.commons.lang.StringUtils;

public class CFMapper extends Mapper<LongWritable, Text, Text, Text> { 
    private static final Text REDUCER_KEY = new Text();
    private static final Text REDUCER_VALUE = new Text();

    static String getFriends(String[] tokens) {
        if (tokens.length == 2) 
            return "";

        StringBuilder builder = new StringBuilder();
        for (int i = 1; i < tokens.length; i++) {
            builder.append(tokens[i]);
            if (i < (tokens.length - 1)) {
                builder.append(",");
            }
        }
        return builder.toString();
    }
    

    static String buildSortedKey(String person, String friend) {
        long p = Long.parseLong(person);
        long f = Long.parseLong(friend);
        if (p < f) {
            return person + "," + friend;
        } else {
            return friend + "," + person;
        }
    }

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // parse input, delimiter is a single space
        String[] tokens = StringUtils.split(value.toString(), " ");
        // create reducer value
        String friends = getFriends(tokens);
        REDUCER_VALUE.set(friends);

        String person = tokens[0];
        for (int i = 1; i < tokens.length; i++) {
            String friend = tokens[i];
            String reducerKeyAsString = buildSortedKey(person, friend);
            REDUCER_KEY.set(reducerKeyAsString);
            context.write(REDUCER_KEY, REDUCER_VALUE);
        }
    }
}
