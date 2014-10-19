#include<iostream>
#include<queue>
#include<cmath>
#include<cassert>
#include<cstdlib>
#include<fstream>
#include<sstream>
#include<vector>
#include<algorithm>

using namespace std;

const int ITERM_SIZE=1682;
const int USER_SIZE=943;
const int V=15;        //ITERM的最近邻居数
const int S=10;        //USER的最近邻居数

struct MyPair{
    int id;
    double value;
    MyPair(int i=0,double v=0):id(i),value(v){}
};

struct cmp{
    bool operator() (const MyPair & obj1,const MyPair & obj2)const{
        return obj1.value < obj2.value;
    }
};

double rate[USER_SIZE][ITERM_SIZE];    //评分矩阵
MyPair nbi[ITERM_SIZE][V];            //存放每个ITERM的最近邻居
MyPair nbu[USER_SIZE][S];            //存放每个USER的最近邻居
double rate_avg[USER_SIZE];            //每个用户的平均评分

//从文件中读入评分矩阵
int readRate(string filename){
    ifstream ifs;
    ifs.open(filename.c_str());
    if(!ifs){
        cerr<<"error:unable to open input file "<<filename<<endl;
        return -1;
    }
    string line;
    while(getline(ifs,line)){
        string str1,str2,str3;
        istringstream strstm(line);
        strstm>>str1>>str2>>str3;
        int userid=atoi(str1.c_str());
        int itermid=atoi(str2.c_str());
        double rating=atof(str3.c_str());
        rate[userid-1][itermid-1]=rating;
        line.clear();
    }
    ifs.close();
    return 0;
}

//计算每个用户的平均评分
void getAvgRate(){
    for(int i=0;i<USER_SIZE;++i){
        double sum=0;
        for(int j=0;j<ITERM_SIZE;++j)
            sum+=rate[i][j];
        rate_avg[i]=sum/ITERM_SIZE;
    }
}

//计算两个向量的皮尔森相关系数
double getSim(const vector<double> &vec1,const vector<double> &vec2){
    int len=vec1.size();
    assert(len==vec2.size());
    double sum1=0;
    double sum2=0;
    double sum1_1=0;
    double sum2_2=0;
    double sum=0;
    for(int i=0;i<len;i++){
        sum+=vec1[i]*vec2[i];
        sum1+=vec1[i];
        sum2+=vec2[i];
        sum1_1+=vec1[i]*vec1[i];
        sum2_2+=vec2[i]*vec2[i];
    }
    double ex=sum1/len;
    double ey=sum2/len;
    double ex2=sum1_1/len;
    double ey2=sum2_2/len;
    double exy=sum/len;
    double sdx=sqrt(ex2-ex*ex);
    double sdy=sqrt(ey2-ey*ey);
    assert(sdx!=0 && sdy!=0);
    double sim=(exy-ex*ey)/(sdx*sdy);
    return sim;
}

//计算每个ITERM的最近邻
void getNBI(){
    for(int i=0;i<ITERM_SIZE;++i){
        vector<double> vec1;
        priority_queue<MyPair,vector<MyPair>,cmp> neighbour;
        for(int k=0;k<USER_SIZE;k++)
            vec1.push_back(rate[k][i]);
        for(int j=0;j<ITERM_SIZE;j++){
            if(i==j)
                continue;
            vector<double> vec2;
            for(int k=0;k<USER_SIZE;k++)
                vec2.push_back(rate[k][j]);
            double sim=getSim(vec1,vec2);
            MyPair p(j,sim);
            neighbour.push(p);
        }
        for(int j=0;j<V;++j){
            nbi[i][j]=neighbour.top();
            neighbour.pop();
        }
    }
}

//预测用户对未评分项目的评分值
double getPredict(const vector<double> &user,int index){
    double sum1=0;
    double sum2=0;
    for(int i=0;i<V;++i){
        int neib_index=nbi[index][i].id;
        double neib_sim=nbi[index][i].value;
        sum1+=neib_sim*user[neib_index];
        sum2+=fabs(neib_sim);
    }
    return sum1/sum2;
}

//计算两个用户的相似度
double getUserSim(const vector<double> &user1,const vector<double> &user2){
    vector<double> vec1;
    vector<double> vec2;
    int len=user1.size();
    assert(len==user2.size());
    for(int i=0;i<len;++i){
        if(user1[i]!=0 || user2[i]!=0){
            if(user1[i]!=0)
                vec1.push_back(user1[i]);
            else
                vec1.push_back(getPredict(user1,i));
            if(user2[i]!=0)
                vec2.push_back(user2[i]);
            else
                vec2.push_back(getPredict(user2,i));
        }
    }
    return getSim(vec1,vec2);
}

//计算每个USER的最近邻
void getNBU(){
    for(int i=0;i<USER_SIZE;++i){
        vector<double> user1;
        priority_queue<MyPair,vector<MyPair>,cmp> neighbour;
        for(int k=0;k<ITERM_SIZE;++k)
            user1.push_back(rate[i][k]);
        for(int j=0;j<USER_SIZE;++j){
            if(j==i)
                continue;
            vector<double> user2;
            for(int k=0;k<ITERM_SIZE;++k)
                user2.push_back(rate[j][k]);
            double sim=getUserSim(user1,user2);
            MyPair p(j,sim);
            neighbour.push(p);
        }
        for(int j=0;j<S;++j){
            nbu[i][j]=neighbour.top();
            neighbour.pop();
        }
    }
}
            
//产生推荐，预测某用户对某项目的评分
double predictRate(int user,int iterm){
    double sum1=0;
    double sum2=0;
    for(int i=0;i<S;++i){
        int neib_index=nbu[user][i].id;
        double neib_sim=nbu[user][i].value;
        sum1+=neib_sim*(rate[neib_index][iterm]-rate_avg[neib_index]);
        sum2+=fabs(neib_sim);
    }
    return rate_avg[user]+sum1/sum2;
}

//测试
int main(){
    string file="/home/orisun/DataSet/movie-lens-100k/u.data";
    if(readRate(file)!=0){
        return -1;
    }
    getAvgRate();
    getNBI();
    getNBU();
    while(1){
        cout<<"please input user index and iterm index which you want predict"<<endl;
        int user,iterm;
        cin>>user>>iterm;
        cout<<predictRate(user,iterm)<<endl;
    }
    return 0;
}