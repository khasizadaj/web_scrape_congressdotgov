import requests
from bs4 import BeautifulSoup
import csv


# first of all, specify where to put the data to be scraped
outdir = 'C://Users//andre//OneDrive//Desktop//Coding_exam//input_files//'
filename='congress.csv'
inputfile=outdir+filename


# Second Step, create an empty list of all the variables that will contain the information scraped and to be used for the analysis

bills = []
bill=[]
bill_names=[]
sponsor_names=[]
sponsor_pms=[]
#party membership
sponsor_states=[]
sponsor_yr_exps=[]
#the years of service in the House
biparts=[]
cosponsors_nrs=[]
avg_yr_exps =[]
bill_introduceds=[]

# base url to be looped for all pages of congresses (year data) and for the whole number of bills
baseURL = 'https://www.congress.gov/search?q=%7B%22source%22%3A%22legislation%22%2C%22congress%22%3A'

congress = ['110', '111', '112', '113', '114', '115', '116']

# Third Step, draw 6 different samples of size 100 from each of the 7 pages of congress year data
for c in congress:
    URL_congress = baseURL + c + '%7D'
    
    # getinng the html text and reading it with beautiful soup
    r = requests.get(URL_congress)
    html = r.text
    soup = BeautifulSoup(r.text, 'html.parser')

    # Find all indiviudal bills segments, and identify the whole number of bills
    bill_segments = soup.findAll('li', {'class' : 'compact'})
    nr_of_bills = len(bill_segments)
    print('Congress: ' + c)

    # Repeat the process for all bills in each congress
    for i in range(0, nr_of_bills):

        # Getting information from the individual bill, and creating a sub-soup for the individual bill:
        print(i)
        bill_soup = bill_segments[i]
        
        # Finding billname, adding it to the list, and then printing it inside the loop
        billname = bill_soup.find('span',{'class' : 'result-heading'}).text
        billname = billname.split(' ')[0]
        bill_names.append(billname)
        print(billname)

        # Getting the year of introduction of the bill
        bill_introduced = bill_soup.find('a', target="_blank").findNext(string = True).findNext(string=True).split('(')[-1].split('/')[-1].split(')')[0] 
        bill_introduceds.append(bill_introduced)


# Fourth Step, getting the sponsor information:
        

        # Finding the html for sponsorname, adding it to its list, and printing it inside the lopp
        sponsorname = bill_soup.find('strong', string = 'Sponsor:').findNext('a').text
        sponsor_names.append(sponsorname)
        print(sponsorname)

        # Getting the link to the sponsor
        sponsorlink = 'http://congress.gov/' + bill_soup.find('strong').findNext(href=True).get('href')
        r = requests.get(sponsorlink)
        sponsors_soup = BeautifulSoup(r.text, 'html.parser')

        # Will give NaN if sponsor has been in both parties, therefore we remove for this possibility with an if statement
        if sponsors_soup.find('th', {'class':'member_party'}) == None:
            sponsor_pm = float('NaN')
        else:
            sponsor_pm = sponsors_soup.find('th', {'class':'member_party'}).findNext('td').text
                
        # Extracting party membership
        sponsor_pms.append(sponsor_pm)

        # Extracting house column of table, and state
        house = sponsors_soup.find('th', {'class':'member_chamber'}).findNext('td').text
        house_split = house.split(',')
        sponsor_states.append(house_split[0])
        #with house split manage to split long strings that contain multiple characters that would stop the code
        #and finally extracting the range in years of service to be used to obtain the career time

        if len(house_split) == 2:
            house_split = house_split[1]

        if len(house_split) == 1:
            house_split = house_split[0]

        service = house_split.split('(')[1]

        # Solving for the exception if sponsor only for one year:
        if len(service.split('-'))==2:    
            start_year = service.split('-')[0]
            start_year = int(start_year)
            end_year = service.split('-')[1].split(')')[0]
        else:
            start_year = service.split(')')[0]
            start_year = int(start_year)
            end_year = start_year + 1

        if end_year == 'Present':
            end_year = '2021'
            
        end_year = int(end_year)

        sponsor_yr_exps.append(end_year - start_year)


# Fifth Step, getting cosponsor information:

       # Finding co-sponsor link, getting the html text, and reading it with beautiful soup
        cosponsorlink = bill_soup.find('strong', string = 'Cosponsors:').findNext('a')
        cosponsorlink = cosponsorlink.get('href')
        cosponsorlink = 'http://congress.gov/' + cosponsorlink
        r = requests.get(cosponsorlink)
        cosponsors_soup = BeautifulSoup(r.text, 'html.parser')

        #finding all cosponsors for each individual bill
        cosponsors = cosponsors_soup.findAll('td', {'class':'actions'})
        cosponsors2 = [cosponsors[i].find(href = True) for i in range(1, len(cosponsors))]
        cosponsorlinks2 = [cosponsors2[k].get('href') for k in range(1, len(cosponsors2))]

        #initialize the lists for cosponsors variables:
        cosponsor_pms=[]
        cosponsor_state = []
        cosponsor_years_service = []
        bipart = 0

# Sixth step, obtaining individual characteristics of the cosponsors

        #loop over all of them
        for j in range(0, len(cosponsorlinks2)):
            
            # Loading html from an individual cosponsor, interpret with beautiful soup
            r = requests.get(cosponsorlinks2[j])
            cosponsor_soup = BeautifulSoup(r.text, 'html.parser')

            # Exactly has with sponsors, it will return NaN if cosponsor has been in both parties
            if cosponsor_soup.find('th', {'class':'member_party'}) == None:
                cosponsor_pm = float('NaN')
            else:
                cosponsor_pm = cosponsor_soup.find('th', {'class':'member_party'}).findNext('td').text
                
            # getting party membership, and extracting years of service in the house exactly as before
            cosponsor_pms.append(cosponsor_pm)
            house = cosponsor_soup.find('th', {'class':'member_chamber'}).findNext('td').text
            
            house_split = house.split(',')
            cosponsor_state.append(house_split[0])

            if len(house_split) == 2:
                house_split = house_split[1]

            if len(house_split) == 1:
                house_split = house_split[0]

            service = house_split.split('(')[1]
            
            # Solving for the exception if sponsor only for one year:
            if len(service.split('-'))==2:    
                start_year = service.split('-')[0]
                start_year = int(start_year)
                end_year = service.split('-')[1].split(')')[0]
            else:
                start_year = service.split(')')[0]
                start_year = int(start_year)
                end_year = start_year + 1

            if end_year == 'Present':
                end_year = '2021'
            
            end_year = int(end_year)

            # transforming the variable into integers and calculating years of service.
            cosponsor_years_service.append(end_year - start_year)
            

# Step Seven, Creating the bipartisan variable to be used for the regression analysis

            if j == 0:
                bipart_comp = cosponsor_pms[j]
                    
            if cosponsor_pms[j] != bipart_comp:
                bipart = 1

        # number of cosponsors of the bill:
        cosponsors_nr = len(cosponsor_pms)

        # Calculates the values if the bill has cosponsors:
        if cosponsors_nr != 0:
            
            avg_yr_exp = sum(cosponsor_years_service)/cosponsors_nr

        # otherwise sets them to nan
        else:
            
            avg_yr_exp = float("nan")
            bipart = float("nan")

        # appending to its lists number of cosponsors, the years of service, and the bipartisan variable   
        cosponsors_nrs.append(cosponsors_nr)
        biparts.append(bipart)
        avg_yr_exps.append(avg_yr_exp)
        
#Step Eight: using zip, create a list in which each element is a "row" of data, and then with csv.writer create the csv file storing the scraped dataset
        
result=zip(bill_names, bill_introduceds, sponsor_names, sponsor_pms, sponsor_states, sponsor_yr_exps, biparts, cosponsors_nrs, avg_yr_exps)

with open(inputfile, 'w', newline='') as f:
    writer=csv.writer(f, delimiter=',')
    writer.writerow(['Bill Name', 'Year Introduced', 'Sponsor', 'Sponsor party membership', 'Sponsor states', 'Sponsors years of service', 'Bipartisan co-sponsorship', 'Nr of co-sponsors', 'Average years of service among co-sponsors'])
    for a in list(result):
        writer.writerow(a)
            

