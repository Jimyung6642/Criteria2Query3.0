package edu.columbia.dbmi.ohdsims.tool;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import edu.columbia.dbmi.ohdsims.pojo.GlobalSetting;
import edu.columbia.dbmi.ohdsims.util.FileUtil;
import org.python.antlr.ast.Str;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class NegationDetection {
    private final String neg_folder = GlobalSetting.negateDetectionFolder;
    private final String file = "negdetect.py";
    private final String virtualEnvFolder = GlobalSetting.virtualEnvFolder;
    private String[] arguments = new String[]{this.virtualEnvFolder+"/python", file, ""};
    Random rand = new Random();
    public static void main(String[] args) {
        //address = System.getProperty("user.dir");//NegationDetection.class.getClassLoader().getResource("test.py").getPath();
        //System.out.println(address);
        NegationDetection nd = new NegationDetection();
        // TODO Auto-generated method stub
        Integer[] cues = new Integer[5];
        for (int i=0; i<5; i++){
            cues[i] = 3;
        }
        List<List<Integer>> negateTags = new ArrayList<>();
        negateTags.add(Arrays.asList(cues));
        cues[1] = 1;
        negateTags.add(Arrays.asList(cues));
        List<String> sents = new ArrayList<>();
        sents.add("["+"They do have diabetes ."+"]");
        sents.add("["+"They don't have diabetes ."+"]");
        List  negate = nd.getNegateTag(negateTags, sents);
        for(int i=0; i<negate.size();i++){
            System.out.println(negate.get(i));
        }
    }

    public List<List<Integer>> getNegateTag(List<List<Integer>> cues, List<String> sents){
        String sents_str = sents.toString();
        sents_str = sents_str.replaceAll("\"", "'");
        String input = sents_str.substring(4, sents_str.length()-4);
        input += "\t";
        String cues_str = cues.toString();
        input += cues_str.substring(2, cues_str.length()-2);
        List<List<Integer>> negates = new ArrayList<>();
        try {
            //long startTime = System.currentTimeMillis();
            Long startTime = System.currentTimeMillis();
            Integer randInt = rand.nextInt();
            String inputFileName = startTime.toString()+"_"+randInt.toString();
            File dir = new File(neg_folder);
            File transfer_data = new File(neg_folder+"/java_python_data_transfer");
            if (transfer_data.isDirectory() == false){
                transfer_data.mkdirs();
            }
            FileUtil.write2File(neg_folder+"/java_python_data_transfer/"+ inputFileName+".txt", input);
            this.arguments[2] = inputFileName;
            Process process = Runtime.getRuntime().exec(this.arguments, null, dir);
            int re = process.waitFor();
            System.out.println(re);
            BufferedReader reader = new BufferedReader(new FileReader(neg_folder+"/java_python_data_transfer/"+ inputFileName+".txt"));
            String tags = reader.readLine();
            reader.close();
            File inputFile = new File(neg_folder+"/java_python_data_transfer/"+ inputFileName+".txt");
            inputFile.delete();
            if(tags != null) {
                String[] tags_group = tags.substring(3, tags.length() - 3).split("(]]|]), (\\[\\[|\\[)");
                for (String tags_per_sent : tags_group) {
                    List<Integer> negates_per_sent = new ArrayList<>();
                    for (String tag: tags_per_sent.split(", ")){
                        negates_per_sent.add(Integer.parseInt(tag));
                    }
                    negates.add(negates_per_sent);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return negates;
    }

}
