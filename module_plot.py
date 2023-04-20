from datetime import datetime, date
import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import pathlib
from module_mail import *
# Define Dataset

def create_visualization(filename,inputActual,inputTarget):
    '''
        Create Visualization
        return:
            - saves the created plot in ./results/
            - dir: path to working directory
            - path: path to saved plot
    '''
    
    Dimension= ("Completeness", "Uniqueness", "Accuracy","Validity")
    
    ActualList=[]
    if 'Completeness' in inputActual:
        ActualList.append(float(inputActual["Completeness"]))
    else:
        ActualList.append(0)

    if 'Uniqueness' in inputActual:
        ActualList.append(float(inputActual["Uniqueness"]))
    else:
        ActualList.append(0)
    if 'Accuracy' in inputActual:
        ActualList.append(float(inputActual["Accuracy"]))
    else:
        ActualList.append(0)
    if 'Validity' in inputActual:
        ActualList.append(float(inputActual["Validity"]))
    else:
        ActualList.append(0)   


    
    TargetList=[]
    if 'Completeness' in inputTarget:
        TargetList.append(float(inputTarget["Completeness"]))
    else:
        TargetList.append(0)

    if 'Uniqueness' in inputTarget:
        TargetList.append(float(inputTarget["Uniqueness"]))
    else:
        TargetList.append(0)
    if 'Accuracy' in inputTarget:
        TargetList.append(float(inputTarget["Accuracy"]))
    else:
        ActualList.append(0)
    if 'Validity' in inputTarget:
        TargetList.append(float(inputTarget["Validity"]))
    else:
        TargetList.append(0)      
    Actual = ActualList 

    Target = TargetList
    print(Actual)
    print(Target)
    x = np.arange(len(Dimension))  # the label locations
    width = 0.25  # the width of the bars
    multiplier = 0

    fig, ax = plt.subplots(layout='constrained')

    # for attribute, measurement in penguin_means.items():
    #     offset = width * multiplier
    #     rects = ax.bar(x + offset, measurement, width, label=attribute)
    #     ax.bar_label(rects, padding=3)
    #     multiplier += 1
    # # Stacked bar chart
    offset = width * multiplier
    rects = ax.bar(x + offset, Actual, width,color = "#024b7a", label = 'Actual') 
    ax.bar_label(rects, padding=3)
    multiplier += 1
    offset = width * multiplier
    rects = ax.bar(x + offset, Target, width, color = "#44a5c2",label = 'Tolerance') 
    ax.bar_label(rects, padding=3)
    
    plt.xticks(x + offset/2,Dimension) 
   

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('Quality Percentage ("%")')
    ax.set_title('Data Quality Analysis Report')
    
    ax.legend(loc='upper left', ncols=3)
    ax.set_ylim(0, 200)
    


    # define filename with current date e.g. "2021-04-08.png"
    filename =filename+ str(date.today()) + ".png"

    # working directory
    dir = pathlib.Path(__file__).parent.absolute()

    

    path_plot = "cleaned_data/"+ filename

    # save plot
    fig.savefig(path_plot, dpi=fig.dpi)

    return path_plot, dir

def SendResultByMail(filename,inputActual,inputTarget,from_mail,  to_mail,MailTemplatePath, FileNameText,FileSubject,flagChart):
    if flagChart=="YES":
        path_plot, dir = create_visualization(filename,inputActual,inputTarget)
        print(path_plot,dir)
        smtp_server = 'smtp.gmail.com'
        smtp_port = 587
        from_password =  'ebncoiuzyfaprukr'
        from_mail = 'suji.arumugam@gmail.com'
        
        
        
        send_email_new(path_plot, from_mail,  to_mail,MailTemplatePath, FileNameText,FileSubject)
    else:
        path_plot, dir = "",""
        smtp_server = 'smtp.gmail.com'
        smtp_port = 587
        from_password =  'ebncoiuzyfaprukr'
        from_mail = 'suji.arumugam@gmail.com'
        
        send_email_withoutChart(path_plot, from_mail,  to_mail,MailTemplatePath, FileNameText,FileSubject)   
    return path_plot,dir