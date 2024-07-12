import subprocess
import os
import urllib.parse
from wsgiref.util import FileWrapper

from django.conf import settings
from django.http import FileResponse, Http404
from django.shortcuts import render, redirect
from django.views.generic import TemplateView
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.http import StreamingHttpResponse
import mimetypes


def download_file(request, file_name):
    decoded_file_name = urllib.parse.unquote(file_name)

    base_dir = settings.BASE_DIR
    file_path = os.path.join(base_dir, 'RightMove Properties Scraper', 'rightmove_properties',
                             'rightmove_properties', 'output', decoded_file_name)

    if not os.path.exists(file_path):
        raise Http404(f'File "{decoded_file_name}" does not exist.')

    # Extract the file name from the path
    file_basename = os.path.basename(file_path)
    chunk_size = 8192
    response = StreamingHttpResponse(FileWrapper(open(file_path, 'rb'),chunk_size),
                                     content_type=mimetypes.guess_type(file_path)[0])
    response['Content-Length'] = os.path.getsize(file_path)
    response['Content-Disposition'] = "Attachment;filename=%s" % file_basename
    return response



class HomePageView(TemplateView):
    """Home page view class"""
    template_name = 'home.html'

    def get(self, request, *args, **kwargs):
        """Handles get requests to '/'"""
        return render(request, self.template_name)

    def post(self, request, *args, **kwargs):
        """Handles POST requests to '/'"""
        urls = []
        if 'urlInput' in request.POST and request.POST['urlInput'].strip():
            input_urls = request.POST.get('urlInput').strip().split('\n')
            input_urls = [url.strip() for url in input_urls]
            urls.extend(input_urls)

        if 'fileInput' in request.FILES:
            uploaded_file = request.FILES['fileInput']
            if uploaded_file.name.endswith('.txt') and uploaded_file.content_type == 'text/plain':
                # Read content from the uploaded file
                file_content = uploaded_file.read().decode('utf-8')
                file_urls = file_content.strip().split('\n')
                file_urls = [url.strip() for url in file_urls]
                urls.extend(file_urls)

        # Save URLs to file
        if urls:
            # Save URLs to a file in the specified directory
            folder_path = os.path.join(settings.BASE_DIR, 'RightMove Properties Scraper', 'rightmove_properties',
                                       'rightmove_properties', 'input')
            if not os.path.exists(folder_path):
                os.makedirs(folder_path)

            file_path = os.path.join(folder_path, 'property_urls.txt')
            with open(file_path, 'w') as file:
                for url in urls:
                    file.write(f"{url}\n")

        # Handle Scraper button clicks and get the generated file name
        if 'csv' in request.POST['outputFormat']:
            file_name = self.handle_scraper(request, key='rightmove')
            # return render(request, self.template_name, {'file_name': file_name})
            return JsonResponse({'file_name': file_name})

        elif 'pdf' in request.POST['outputFormat']:
            pdf_files = self.handle_scraper(request, key='rightmove_pdf')
            if isinstance(pdf_files, JsonResponse):
                return pdf_files

    def handle_scraper(self, request, key):
        button_type = 'CSV' if 'rightmove' == key else 'PDF'
        try:
            # Navigate to the Scrapy project directory
            base_dir = settings.BASE_DIR
            scrapy_project_dir = os.path.join(base_dir, 'RightMove Properties Scraper', 'rightmove_properties',
                                              'rightmove_properties')

            # Command to run the Scrapy spider
            process_args = ["scrapy", "crawl", key]
            subprocess.run(process_args, check=True,
                           cwd=scrapy_project_dir)  # Use check=True to raise an error if the command fails

            output_dir = os.path.join(scrapy_project_dir, 'output')

            if button_type == 'CSV':
                latest_file = max([os.path.join(output_dir, f) for f in os.listdir(output_dir) if f.endswith('.xlsx')], key=os.path.getctime)
                return latest_file
            else:
                latest_dir = max([os.path.join(output_dir, d) for d in os.listdir(output_dir) if
                                  os.path.isdir(os.path.join(output_dir, d))],
                                 key=os.path.getctime)
                pdf_files = [os.path.join(latest_dir, f) for f in os.listdir(latest_dir) if f.endswith('.pdf')]

                if not pdf_files:
                    return JsonResponse({'message': 'No PDF files found.'}, status=404)

                return JsonResponse({'pdf_files': pdf_files})

        except subprocess.CalledProcessError as e:
            print(f"Error occurred while running Scrapy spider: {e}")
            return HttpResponse(f"Failed to initiate {button_type} download.", status=500)
        except Exception as e:
            print(f"Error occurred: {e}")
            return HttpResponse(f"An error occurred while retrieving the {button_type} file.", status=500)