public class Main {
    public static void main(String args[]) {
        Genom genom1 = new Genom("gene1_2", "Genome_1-1.txt");
        genom1.run(5);

        Genom genom2 = new Genom("gene2_5", "Genome_2-1.txt");
        genom2.run(5);

        System.out.println(genom1.getAnswer("gene1_5", "gene2_5"));
    }
}
