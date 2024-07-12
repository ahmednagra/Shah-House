from django.shortcuts import render
import pandas as pd
from .right_move import main
from django.http import HttpResponse


def home(request):
    if request.method == 'POST':
        # Get the URL from the form submission
        url = request.POST.get('url')

        try:
            data = main(url)

            if not data:
                return render(request, 'home.html', {'error_message': f'Failed to process URL: {url}'})

            df = pd.DataFrame(data)
            excel_file_path = 'property_data.xlsx'
            df.to_excel(excel_file_path, index=False)

            # Optionally, you can send the file for download directly
            # with open(excel_file_path, 'rb') as f:
            #     response = HttpResponse(f.read(), content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            #     response['Content-Disposition'] = 'attachment; filename=property_data.xlsx'
            #     return response

            return render(request, 'home.html', {'excel_file_path': excel_file_path})

        except Exception as e:
            return render(request, 'home.html', {'error_message': f'Error processing URL {url}: {e}'})

    return render(request, 'home.html')
