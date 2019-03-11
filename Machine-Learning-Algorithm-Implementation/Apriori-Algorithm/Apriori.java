import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Apriori {
    
    public static ArrayList<String> FirstPass(double Threshold) throws IOException{
        
        BufferedReader br = new BufferedReader(new FileReader("userFeature.data"));
        HashMap<String, Integer> ItemsSet = new HashMap<String, Integer>();
        int TotalUser = 0;
        String line;
            
        /* Count the Single Items */
            
        while ((line = br.readLine()) != null) {
                
            /* Generate the User Item and Count */
                
            TotalUser++;
            ArrayList<String> UserInterest = new ArrayList();

            String Regex = "interest?(.*?)\\|";

            Matcher InterestMatch = Pattern.compile(Regex).matcher(line);

            while(InterestMatch.find()){
                UserInterest.add(InterestMatch.group());
            }

            for(String s : UserInterest){

                String[] Splitted = s.replaceAll("\\|","").split(" ");

                for(int i = 1; i < Splitted.length; i++){

                    StringBuffer Item = new StringBuffer();
                    Item.append(Splitted[0] + ":" + Splitted[i]);   
                    Integer Count = ItemsSet.get(Item.toString());
                    if(Count == null) Count = new Integer(0);
                    Count++;
                    ItemsSet.put(Item.toString(), Count);   

                }

            }
        
        }
        
        /* Find the Frequent Single Items */
        
        Set<String> Keys = ItemsSet.keySet();
        ArrayList<String> CandidateSet = new ArrayList();
        for (String s : Keys) {
            double Support = (double)ItemsSet.get(s)/(double)TotalUser;
            if(Support >= Threshold) CandidateSet.add(s);
        }
        
        /* Sort by Number */
        
        Collections.sort(CandidateSet);
        
        return CandidateSet;
        
    }
    
    public static ArrayList<String> SecondPass(ArrayList<String> CandidateSet, double Threshold) throws IOException{
        
        /* Initialize the Candidate Pair */
        
        HashMap<String, Integer> CandidatePairSet = new HashMap<String, Integer>();
        
        for(int i = 0; i < CandidateSet.size() - 1; i++){
            for(int j = i + 1; j < CandidateSet.size(); j++){
                String CandidatePair = CandidateSet.get(i) + " " + CandidateSet.get(j);
                CandidatePairSet.put(CandidatePair, new Integer(0)); 
            }
        }
        
        BufferedReader br = new BufferedReader(new FileReader("userFeature.data"));
        String line;
        int TotalUser = 0;
        Set<String> CandidatePairKeys = CandidatePairSet.keySet();
        
        /* For each Record, Count the Frequency in the Candidate Pair */
        
        while ((line = br.readLine()) != null) {
                
                /* Generate the User Items */
            
                TotalUser++;
                ArrayList<String> UserInterest  = new ArrayList();
                ArrayList<String> UserItemSet  = new ArrayList();
                
                String Regex = "interest?(.*?)\\|";
                Matcher InterestMatch = Pattern.compile(Regex).matcher(line);

                while(InterestMatch.find()){
                    UserInterest.add(InterestMatch.group());
                }

                for(String s : UserInterest){

                    String[] Splitted = s.replaceAll("\\|","").split(" ");     

                    for(int i = 1; i < Splitted.length; i++){

                        StringBuffer Item = new StringBuffer();
                        Item.append(Splitted[0] + ":" + Splitted[i]);
                        if(CandidateSet.contains(Item.toString()))
                            UserItemSet.add(Item.toString());

                    }
                    
                }
                
                Collections.sort(UserItemSet);
                
                /* Count the Frequency */
                    
                for(int i = 0; i < UserItemSet.size() - 1; i++){
                    for(int j = i + 1; j < UserItemSet.size(); j++){
                        String UserItemSetPair = UserItemSet.get(i) + " " + UserItemSet.get(j);
                        Integer Count = CandidatePairSet.get(UserItemSetPair);
                        if(Count == null) continue;
                        Count++;
                        CandidatePairSet.put(UserItemSetPair, Count);
                    }
                }
                
        }
        
        /* Find the Frequent Pair */
        
        ArrayList<String>  FrequentPairSet = new ArrayList();
        
        for (String s : CandidatePairKeys) {
            double Support = (double)CandidatePairSet.get(s)/(double)TotalUser;
            if(Support >= Threshold)
               FrequentPairSet.add(CandidatePairSet.get(s) + " " + s);
        }
            
        return FrequentPairSet;        
        
    }
    
    public static void main(String[] args) throws IOException{
        
        double Threshold = Double.parseDouble(args[0]);
        
        ArrayList<String> FrequentItemSet = FirstPass(Threshold);
        System.out.printf("First Pass Done ...\n");
        
        ArrayList<String> FrequentPairSet = SecondPass(FrequentItemSet, Threshold);
        System.out.printf("Second Pass Done ... \n");
        
        FileWriter fw = new FileWriter("out.txt");
        String newLine = System.getProperty("line.separator");
        for(String s : FrequentPairSet)
           fw.write(s + newLine);
        fw.close();
        
    }
    
}