# I forgot this in interview. I m Moron
def maxValueReplace(str):
    return str.replace("6","9",1)

def maxValue(str):
    flag=0
    for i in range(len(str)):
        if str[i]=="6":
            flag=1
            break
    if flag==1:
        # I made a mistake here. Missed =
        if i<=len(str)-1:
            # Made a mistak
            return str[:i]+"9"
        else:
            return str[:i]+"9"+str[i:]
    else:
        return str
    
str = "9669"
str2= "9999"
str3= "996699"
str4= "9996"

print(maxValue(str4))

